from pyspark.sql.functions import col, count, when, isnan
import logging

logger = logging.getLogger(__name__)


class Validacao:
    """
    Classe para validação e métricas de qualidade de dados.
    Gera relatórios de completude, duplicatas e estatísticas.
    """

    def __init__(self, df):
        """
        Inicializa a classe com um DataFrame Spark.
        
        Parâmetros:
            df (pyspark.sql.DataFrame): DataFrame a ser validado.
        """
        self.df = df

    def relatorio_qualidade(self, coluna_chave="PRONTUARIO"):
        """
        Gera relatório completo de qualidade dos dados.
        
        Parâmetros:
            coluna_chave (str): Coluna a ser usada para verificar duplicatas.
        
        Retorna:
            dict: Dicionário com todas as métricas calculadas.
        """
        logger.info("📊 === INICIANDO RELATÓRIO DE QUALIDADE DOS DADOS ===")
        
        metricas = {}
        
        # 1. Completude
        logger.info("\n🔍 === MÉTRICAS DE COMPLETUDE ===")
        metricas['completude'] = self._metricas_completude()
        
        # 2. Duplicatas
        logger.info("\n🔍 === VERIFICAÇÃO DE DUPLICATAS ===")
        metricas['duplicatas'] = self._verificar_duplicatas(coluna_chave)
        
        # 3. Estatísticas básicas
        logger.info("\n📈 === ESTATÍSTICAS DESCRITIVAS ===")
        self._estatisticas_basicas()
        
        logger.info("\n✅ Relatório de qualidade concluído!")
        
        return metricas

    def _metricas_completude(self):
        """
        Calcula percentual de valores nulos por coluna.
        
        Retorna:
            dict: Métricas de completude por coluna.
        """
        total = self.df.count()
        metricas = {}
        
        for coluna in self.df.columns:
            # Verificar se a coluna é numérica antes de aplicar isnan
            col_type = dict(self.df.dtypes)[coluna]
            
            if col_type in ('double', 'float', 'int', 'bigint', 'smallint', 'tinyint'):
                # Para colunas numéricas, verificar nulos e NaN
                nulos = self.df.filter(
                    col(coluna).isNull() | isnan(col(coluna))
                ).count()
            else:
                # Para colunas não numéricas, verificar apenas nulos
                nulos = self.df.filter(col(coluna).isNull()).count()
            
            percentual_nulos = (nulos / total) * 100 if total > 0 else 0
            preenchidos = total - nulos
            percentual_preenchidos = 100 - percentual_nulos
            
            metricas[coluna] = {
                'total': total,
                'nulos': nulos,
                'preenchidos': preenchidos,
                'percentual_nulos': round(percentual_nulos, 2),
                'percentual_preenchidos': round(percentual_preenchidos, 2)
            }
            
            # Log formatado
            status = "✅" if percentual_nulos == 0 else "⚠️" if percentual_nulos < 10 else "❌"
            logger.info(
                f"  {status} {coluna:25} | "
                f"Preenchidos: {percentual_preenchidos:6.2f}% ({preenchidos:4}/{total:4}) | "
                f"Nulos: {percentual_nulos:5.2f}%"
            )
        
        return metricas

    def _verificar_duplicatas(self, coluna_chave):
        """
        Verifica registros duplicados com base em uma coluna chave.
        
        Parâmetros:
            coluna_chave (str): Nome da coluna chave para verificação.
        
        Retorna:
            dict: Métricas sobre duplicatas.
        """
        total = self.df.count()
        distintos = self.df.select(coluna_chave).distinct().count()
        duplicados = total - distintos
        percentual_duplicados = (duplicados / total) * 100 if total > 0 else 0
        
        metricas = {
            'total': total,
            'unicos': distintos,
            'duplicados': duplicados,
            'percentual_duplicados': round(percentual_duplicados, 2)
        }
        
        status = "✅" if duplicados == 0 else "⚠️"
        logger.info(f"  {status} Total de registros: {total}")
        logger.info(f"  {status} Registros únicos: {distintos}")
        logger.info(f"  {status} Registros duplicados: {duplicados} ({percentual_duplicados:.2f}%)")
        
        return metricas

    def _estatisticas_basicas(self):
        """
        Mostra estatísticas descritivas para colunas numéricas.
        """
        # Identificar colunas numéricas
        colunas_numericas = [
            field.name for field in self.df.schema.fields 
            if str(field.dataType) in ['DoubleType', 'IntegerType', 'FloatType', 'LongType']
        ]
        
        if colunas_numericas:
            logger.info(f"  Colunas numéricas analisadas: {', '.join(colunas_numericas)}")
            self.df.select(colunas_numericas).describe().show()
        else:
            logger.info("  ⚠️ Nenhuma coluna numérica encontrada para estatísticas.")

    def validar_faixa_valores(self, coluna, min_val, max_val):
        """
        Valida se valores de uma coluna estão dentro de uma faixa esperada.
        
        Parâmetros:
            coluna (str): Nome da coluna a validar.
            min_val (float): Valor mínimo esperado.
            max_val (float): Valor máximo esperado.
        
        Retorna:
            dict: Métricas de validação da faixa.
        """
        total = self.df.count()
        invalidos = self.df.filter(
            (col(coluna) < min_val) | (col(coluna) > max_val)
        ).count()
        validos = total - invalidos
        percentual_invalidos = (invalidos / total) * 100 if total > 0 else 0
        
        metricas = {
            'coluna': coluna,
            'faixa': f'{min_val} - {max_val}',
            'total': total,
            'validos': validos,
            'invalidos': invalidos,
            'percentual_invalidos': round(percentual_invalidos, 2)
        }
        
        status = "✅" if invalidos == 0 else "⚠️"
        logger.info(
            f"  {status} {coluna}: {validos}/{total} valores dentro da faixa "
            f"[{min_val}, {max_val}] ({100 - percentual_invalidos:.2f}%)"
        )
        
        return metricas