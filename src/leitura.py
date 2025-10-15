from pyspark.sql import SparkSession, DataFrame
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class Leitura:
    """
    Classe responsável pela leitura de dados a partir de múltiplas fontes usando PySpark.

    Suporta leitura de arquivos em diversos formatos (CSV, JSON, Parquet, XML) e
    também conexões JDBC (SQLite, PostgreSQL, MySQL, SQL Server).
    """

    def __init__(self, spark: SparkSession, atributos: Dict[str, Any]) -> None:
        """
        Inicializa a classe Leitura com os parâmetros de configuração.

        Args:
            spark (SparkSession): Sessão Spark ativa.
            atributos (dict): Dicionário de configuração, geralmente carregado de um YAML.
                Exemplo:
                {
                    "source": {
                        "file_type": "parquet",
                        "file_path": "/caminho/entrada/",
                        "options": {"header": True}
                    }
                }
        """
        # Sessão Spark para leitura de dados
        self.spark: SparkSession = spark

        # Configurações de leitura
        self.atributos: Dict[str, Any] = atributos
        self.tipo_source: str = atributos["source"].get("file_type", "").lower()
        self.options_source: Dict[str, Any] = atributos["source"].get("options", {})
        self.caminho_source: str = atributos["source"]["file_path"]

        # DataFrame resultante após a leitura
        self.df: Optional[DataFrame] = None

    def ler(self) -> DataFrame:
        """
        Lê dados a partir do caminho ou conexão configurada e retorna um DataFrame Spark.

        Suporta os formatos:
        - Arquivos: CSV, JSON, Parquet, XML
        - JDBC: SQLite, PostgreSQL, MySQL, SQL Server

        Returns:
            DataFrame: DataFrame contendo os dados lidos.

        Raises:
            ValueError: Caso o tipo de source não seja suportado.
            Exception: Para erros genéricos de leitura.
        """
        try:
            logger.info(f"📂 Iniciando leitura do tipo '{self.tipo_source}'")

            # --- Leitura de arquivos locais ou distribuídos ---
            if self.tipo_source in ("csv", "xml", "parquet", "json"):
                logger.info(f"📁 Caminho de leitura: {self.caminho_source}")
                self.df = (
                    self.spark.read.format(self.tipo_source)
                    .options(**self.options_source)
                    .load(self.caminho_source)
                )

            # --- Leitura via JDBC ---
            elif self.tipo_source in ("sqlite", "postgresql", "mysql", "sqlserver"):
                tabela = self.atributos.get("tabela")
                driver = self.atributos.get("driver")
                conexao = self.atributos.get("conexao")

                if not all([tabela, driver, conexao]):
                    raise ValueError("⚠️ Parâmetros JDBC incompletos no arquivo de configuração.")

                jdbc_options = {"url": conexao, "dbtable": tabela, "driver": driver}

                logger.info(f"🗄️ Conectando ao banco via JDBC ({self.tipo_source})...")
                self.df = self.spark.read.format("jdbc").options(**jdbc_options).load()

            else:
                raise ValueError(
                    f"Tipo de source '{self.tipo_source}' não suportado. "
                    "Use 'csv', 'json', 'parquet', 'xml' ou fontes JDBC."
                )

            logger.info("✅ Leitura concluída com sucesso!")
            return self.df

        except Exception as e:
            logger.error(f"❌ Erro ao ler os dados: {e}")
            raise
