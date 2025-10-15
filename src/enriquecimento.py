from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_date,
    date_diff,
    current_date,
    split,
)
from pyspark.sql.types import IntegerType


class Enriquecimento:
    """
    Classe responsável pelo enriquecimento de dados em um DataFrame do PySpark.

    Essa classe contém métodos para derivar novas colunas, como idade a partir de datas
    e separação de pressões arteriais em componentes sistólica e diastólica.
    """

    def __init__(self, df: DataFrame) -> None:
        """
        Inicializa a classe Enriquecimento.

        Args:
            df (DataFrame): DataFrame de entrada que será enriquecido.
        """
        self.df: DataFrame = df

    def gera_idade(
        self,
        coluna_data_aniversario: str,
        nome_coluna_saida: str = "IDADE_ATUAL",
        formato_data: str = "dd/MM/yyyy",
    ) -> DataFrame:
        """
        Calcula a idade em anos com base na data de aniversário informada.

        A função converte a coluna de data para o formato especificado e calcula
        a diferença em dias entre a data atual e a data de nascimento, dividindo por 365.

        Args:
            coluna_data_aniversario (str): Nome da coluna contendo a data de aniversário.
            nome_coluna_saida (str, optional): Nome da coluna resultante com a idade. Default: "IDADE_ATUAL".
            formato_data (str, optional): Formato da data na coluna de entrada. Default: "dd/MM/yyyy".

        Returns:
            DataFrame: DataFrame com a nova coluna de idade adicionada.
        """
        return self.df.withColumn(
            nome_coluna_saida,
            (
                date_diff(
                    current_date(),
                    to_date(col(coluna_data_aniversario), formato_data),
                )
                / 365
            ).cast(IntegerType()),
        )

    def gera_pressao_sistolica_diastolica(
        self,
        coluna_pressao: str,
        nome_sistolica: str = "PRESSAO_SISTOLICA",
        nome_diastolica: str = "PRESSAO_DIASTOLICA",
    ) -> DataFrame:
        """
        Separa a pressão arterial em duas colunas: sistólica e diastólica.

        A função divide os valores da coluna de pressão (no formato "120/80") em duas novas colunas.

        Args:
            coluna_pressao (str): Nome da coluna contendo a pressão arterial no formato "sistólica/diastólica".
            nome_sistolica (str, optional): Nome da coluna resultante para pressão sistólica. Default: "PRESSAO_SISTOLICA".
            nome_diastolica (str, optional): Nome da coluna resultante para pressão diastólica. Default: "PRESSAO_DIASTOLICA".

        Returns:
            DataFrame: DataFrame com as novas colunas de pressão sistólica e diastólica.
        """
        df_temp = self.df.withColumn("_PRESSAO_SPLIT", split(col(coluna_pressao), "/"))
        df_temp = df_temp.withColumn(nome_sistolica, col("_PRESSAO_SPLIT").getItem(0))
        df_temp = df_temp.withColumn(nome_diastolica, col("_PRESSAO_SPLIT").getItem(1))
        df_temp = df_temp.drop("_PRESSAO_SPLIT")
        return df_temp
