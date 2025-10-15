from pyspark.sql import SparkSession, DataFrame
import logging
from typing import Optional, Union, List, Dict, Any

logger = logging.getLogger(__name__)


class Gravacao:
    """
    Classe responsável pela gravação de dados em arquivos usando PySpark.

    Essa classe abstrai a lógica de gravação de DataFrames, permitindo o controle de:
    - Tipo de arquivo (parquet, csv, json, etc.)
    - Número de partições de saída
    - Particionamento físico por colunas
    """

    def __init__(self, spark: SparkSession, atributos: Dict[str, Any]) -> None:
        """
        Inicializa a classe Gravacao com as configurações fornecidas.

        Args:
            spark (SparkSession): Sessão Spark ativa.
            atributos (dict): Dicionário de configuração contendo informações de destino,
                tipicamente carregado de um arquivo YAML. Exemplo:
                {
                    "destiny": {
                        "file_type": "parquet",
                        "file_path": "/caminho/saida/",
                        "options": {"header": True}
                    }
                }
        """
        # Guarda a sessão Spark para leitura e gravação
        self.spark: SparkSession = spark

        # Guarda o dicionário de configuração carregado do YAML
        self.atributos: Dict[str, Any] = atributos

        # Tipo de arquivo de saída (ex: 'json', 'csv', 'parquet', etc.)
        self.tipo_destiny: str = atributos["destiny"].get("file_type", "").lower()
        print(f"self.tipo_destiny: {self.tipo_destiny} ############################")

        # Opções adicionais para escrita (ex: header, delimiter, compression, etc.)
        self.options_destiny: Dict[str, Any] = atributos["destiny"].get("options", {})

        # Caminho do arquivo de destino
        self.caminho_destiny: str = atributos["destiny"]["file_path"]

        # DataFrame a ser gravado (atributo armazenado após chamada do método gravar)
        self.df: Optional[DataFrame] = None

    def gravar(
        self,
        df: DataFrame,
        num_particoes: Optional[int] = None,
        particionar_por: Optional[Union[str, List[str]]] = None,
    ) -> None:
        """
        Grava um DataFrame em arquivo com controle de particionamento e formato.

        Este método permite gravar arquivos únicos, múltiplos ou particionados por colunas.
        Suporta os formatos: parquet, csv, json e xml.

        Args:
            df (DataFrame): DataFrame a ser gravado.
            num_particoes (int, optional): Define o número de partições de saída.
                - Se `1`: Gera um único arquivo (usa `.coalesce(1)`).
                - Se `> 1`: Gera múltiplos arquivos (usa `.repartition(n)`).
                - Se `None`: Usa particionamento padrão do Spark.
            particionar_por (str | list[str], optional): Nome(s) de coluna(s) para particionamento físico.
                Cria subpastas com base nos valores únicos dessas colunas.

        Returns:
            None

        Raises:
            Exception: Caso ocorra erro durante a gravação do DataFrame.

        Exemplo:
            >>> gravador.gravar(df, num_particoes=1)
            >>> gravador.gravar(df, num_particoes=4)
            >>> gravador.gravar(df, particionar_por="ANO")
        """
        self.df = df

        try:
            logger.info(f"💾 Preparando gravação em {self.caminho_destiny}")

            # Aplicar controle de particionamento, se especificado
            if num_particoes == 1:
                logger.info("📄 Gerando arquivo único (coalesce)...")
                self.df = self.df.coalesce(1)
            elif num_particoes and num_particoes > 1:
                logger.info(f"📑 Dividindo em {num_particoes} partições (repartition)...")
                self.df = self.df.repartition(num_particoes)
            else:
                logger.info("📊 Usando particionamento padrão do Spark...")

            # Validação do formato
            if self.tipo_destiny not in ("csv", "xml", "parquet", "json"):
                raise ValueError(
                    f"Tipo de arquivo '{self.tipo_destiny}' não suportado. "
                    "Use 'csv', 'json', 'parquet' ou 'xml'."
                )

            # Configuração inicial do writer
            writer = self.df.write.format(self.tipo_destiny).mode("overwrite")

            # Adiciona particionamento físico, se solicitado
            if particionar_por:
                if isinstance(particionar_por, str):
                    particionar_por = [particionar_por]
                logger.info(f"📂 Particionando por: {', '.join(particionar_por)}")
                writer = writer.partitionBy(*particionar_por)

            # Aplicar opções adicionais e gravar
            writer.options(**self.options_destiny).save(self.caminho_destiny)
            logger.info("✅ Dados gravados com sucesso!")

        except Exception as e:
            logger.error(f"❌ Erro ao gravar dados: {e}")
            raise
