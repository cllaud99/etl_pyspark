from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    initcap,
    trim,
    to_date,
    date_format,
    regexp_replace,
    translate,
)
from pyspark.sql.types import DataType
from typing import Union


class Transformacao:
    """
    Classe responsável por aplicar transformações em colunas de um DataFrame Spark,
    incluindo normalização de textos, formatação de datas e conversão de tipos.
    """

    def __init__(self, df: DataFrame) -> None:
        """
        Inicializa a classe Transformacao com o DataFrame alvo.

        Args:
            df (DataFrame): DataFrame sobre o qual as transformações serão aplicadas.
        """
        self.df: DataFrame = df

    def converter_tipo(self, coluna: str, tipo: Union[str, DataType]) -> DataFrame:
        """
        Converte o tipo de uma coluna para o tipo especificado.

        Args:
            coluna (str): Nome da coluna a ser convertida.
            tipo (str | DataType): Tipo alvo. Pode ser uma string como 'integer', 'double', 'date',
                ou um tipo do módulo `pyspark.sql.types`.

        Returns:
            DataFrame: DataFrame com a coluna convertida para o novo tipo.

        Example:
            >>> transformador.converter_tipo('idade', 'integer')
        """
        if coluna not in self.df.columns:
            raise ValueError(f"Coluna '{coluna}' não encontrada no DataFrame!")

        return self.df.withColumn(coluna, col(coluna).cast(tipo))

    def normaliza_texto_simples(self, coluna: str) -> DataFrame:
        """
        Normaliza texto de uma coluna removendo espaços, convertendo para minúsculas e
        eliminando espaços extras entre palavras.

        Args:
            coluna (str): Nome da coluna a ser normalizada.

        Returns:
            DataFrame: DataFrame com a coluna normalizada.
        """
        if coluna not in self.df.columns:
            raise ValueError(f"Coluna '{coluna}' não encontrada no DataFrame!")

        return (
            self.df.withColumn(coluna, trim(col(coluna)))
            .withColumn(coluna, regexp_replace(col(coluna), r"\\s+", " "))
            .withColumn(coluna, lower(col(coluna)))
        )

    def normaliza_nomes(self, coluna: str) -> DataFrame:
        """
        Normaliza nomes, removendo espaços, eliminando duplicidades e aplicando formatação Title Case.

        Args:
            coluna (str): Nome da coluna a ser normalizada.

        Returns:
            DataFrame: DataFrame com a coluna normalizada em formato Title Case.
        """
        if coluna not in self.df.columns:
            raise ValueError(f"Coluna '{coluna}' não encontrada no DataFrame!")

        return (
            self.df.withColumn(coluna, trim(col(coluna)))
            .withColumn(coluna, regexp_replace(col(coluna), r"\\s+", " "))
            .withColumn(coluna, initcap(col(coluna)))
        )

    def normaliza_nomes_remove_acentuacao(self, coluna: str) -> DataFrame:
        """
        Normaliza nomes removendo espaços, acentuação e aplicando Title Case.

        Args:
            coluna (str): Nome da coluna a ser normalizada.

        Returns:
            DataFrame: DataFrame com nomes normalizados e sem acentuação.
        """
        if coluna not in self.df.columns:
            raise ValueError(f"Coluna '{coluna}' não encontrada no DataFrame!")

        letras_acentuadas = "áãàâÁÃÂÀéèêÉÈÊíìîÍÌÎóòõôÓÒÔÕúùûÚÙÛçÇ"
        letras_sem_acento = "aaaaAAAAeeeEEEiiiIIIooooOOOOuuuUUUcC"

        return (
            self.df.withColumn(coluna, trim(col(coluna)))
            .withColumn(coluna, regexp_replace(col(coluna), r"\\s+", " "))
            .withColumn(coluna, initcap(col(coluna)))
            .withColumn(
                coluna, translate(col(coluna), letras_acentuadas, letras_sem_acento)
            )
        )

    def formatar_datas(
        self, coluna_data: str, formato_origem: str, formato_destino: str
    ) -> DataFrame:
        """
        Formata uma coluna de datas para um novo formato.

        Args:
            coluna_data (str): Nome da coluna de data a ser formatada.
            formato_origem (str): Formato original da data (ex: 'dd/MM/yyyy').
            formato_destino (str): Novo formato desejado (ex: 'yyyy-MM-dd').

        Returns:
            DataFrame: DataFrame com a coluna formatada no novo padrão.
        """
        if coluna_data not in self.df.columns:
            raise ValueError(f"Coluna '{coluna_data}' não encontrada no DataFrame!")

        return self.df.withColumn(
            coluna_data,
            date_format(to_date(col(coluna_data), formato_origem), formato_destino),
        )
