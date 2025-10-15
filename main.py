# PySpark
# Este arquivo foi adaptado para rodar APENAS em containers Docker
import os
import yaml
import glob
import shutil
from datetime import datetime
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
import logging

# Imports dos seus módulos
from src.leitura import Leitura
from src.transformacoes import Transformacao
from src.enriquecimento import Enriquecimento
from src.gravacao import Gravacao
from src.validacao import Validacao

# Configure o logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)    

# diretório onde serão salvos os arquivos dos jars
jar_dir = "jars"

# cria o diretório caso não exista
os.makedirs(jar_dir, exist_ok=True)

# realiza o download dos arquivos jar
urllib.request.urlretrieve(
    "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.36.0.3/sqlite-jdbc-3.36.0.3.jar",
    os.path.join(jar_dir, "sqlite-jdbc-3.36.0.3.jar"),
)


# Função para carregar parâmetros do arquivo YAML
def load_parameters(file_path="paramenters.yml"):
    """
    Carrega os parâmetros de configuração do arquivo YAML
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
        return config["attributes"]
    except FileNotFoundError:
        logger.info(f"Arquivo {file_path} não encontrado!")
        return None
    except yaml.YAMLError as e:
        logger.info(f"Erro ao processar o arquivo YAML: {e}")
        return None


# Carrega os parâmetros do arquivo YAML
attributes = load_parameters()

# Se necessário, adiciona a data atual para cálculo de idade
if (
    attributes
    and "enrichment" in attributes
    and "date_diff_years" in attributes["enrichment"]
):
    for item in attributes["enrichment"]["date_diff_years"]:
        if "end_date" not in item:
            item["end_date"] = datetime.now().strftime("%Y-%m-%d")


def main():
    """
    Função principal que orquestra todo o pipeline ETL
    """
    logger.info("🚀 Iniciando Pipeline ETL")
    logger.info("=" * 40)

    # 1. CRIAR SPARK SESSION
    logger.info("⚡ Criando SparkSession...")
    spark = (
        SparkSession.builder.appName("ETL Pipeline")
        .config("spark.jars", "./jars/sqlite-jdbc-3.36.0.3.jar")
        .config("spark.driver.extraClassPath", "./jars/sqlite-jdbc-3.36.0.3.jar")
        .master("local[*]")
        .getOrCreate()
    )

    logger.info(f"✅ Spark criado! Versão: {spark.version}")
    # Reduzir o nível de log do Spark para WARN
    spark.sparkContext.setLogLevel("WARN")

    # 2. CRIAR OBJETO DA SUA CLASSE
    logger.info("📚 Criando objeto LeituraGravacao...")
    leitor_gravador = Leitura(spark, attributes)

    logger.info("✅ Objeto criado!")

    # 3. LER DADOS
    logger.info("📂 Lendo dados...")
    df = leitor_gravador.ler().cache()

    # Deduplicação
    df_count_antes = df.count()
    df = df.dropDuplicates(["PRONTUARIO"])
    df_count_depois = df.count()
    duplicatas_removidas = df_count_antes - df_count_depois

    if duplicatas_removidas > 0:
        logger.warning(f"⚠️ Removidas {duplicatas_removidas} duplicatas ({duplicatas_removidas/df_count_antes*100:.2f}%)")

    logger.info(f"✅ Dados lidos: {df_count_depois} registros únicos")
    df.show(5)

    logger.info(f"✅ Dados lidos: {df.count()} registros")
    df.show(5)

    # 4. TRANSFORMAÇÕES
    logger.info("🔄 Aplicando transformações...")
    transformacao = Transformacao(df)
    transformacao.normaliza_texto_simples("NOME")
    transformacao.formatar_datas("DATANASCIMENTO", "dd/MM/yyyy", "yyyy-MM-dd")
    df_transformado = transformacao.df  # Pegar resultado final

    # 4.1. CONVERSÃO DE TIPOS PARA DOUBLE
    logger.info("🔢 Convertendo colunas numéricas para double...")

    # colunas_double = ["TEMPERATURA", "ALTURA", "PESO", "SATURACAOOXIGENIO"]
    colunas_double = attributes.get("type_conversion", {}).get("double_columns", [])
    for coluna in colunas_double:
        if coluna in df_transformado.columns:
            df_transformado = df_transformado.withColumn(
                coluna, df_transformado[coluna].cast(DoubleType())
            )

    # 5. ENRIQUECIMENTO
    logger.info("📈 Aplicando enriquecimento...")
    enriquecimento = Enriquecimento(df_transformado)
    df_enriquecido = enriquecimento.gera_pressao_sistolica_diastolica("PRESSAOARTERIAL")
    df_enriquecido = df_enriquecido.drop("PRESSAOARTERIAL")
    
    # 5.1. ADICIONAR COLUNA DE DATA DE PROCESSAMENTO
    from pyspark.sql.functions import current_date, current_timestamp
    df_enriquecido = df_enriquecido.withColumn("DATA_PROCESSAMENTO", current_date())
    df_enriquecido = df_enriquecido.withColumn("TIMESTAMP_PROCESSAMENTO", current_timestamp())
    
    df_enriquecido.show(5)

    # 6. VALIDAÇÃO DE QUALIDADE
    logger.info("🔍 === VALIDAÇÃO DE QUALIDADE DOS DADOS ===")
    validador = Validacao(df_enriquecido)
    
    # Gerar relatório completo
    coluna_chave = attributes.get("quality", {}).get("coluna_chave", "PRONTUARIO")
    metricas = validador.relatorio_qualidade(coluna_chave=coluna_chave)
    
    # Validações de faixa (se configuradas)
    validacoes_faixa = attributes.get("quality", {}).get("validacoes_faixa", [])
    if validacoes_faixa:
        logger.info("\n🔍 === VALIDAÇÃO DE FAIXAS DE VALORES ===")
        for validacao in validacoes_faixa:
            validador.validar_faixa_valores(
                validacao["coluna"],
                validacao["min"],
                validacao["max"]
            )
    
    # 7. GRAVAÇÃO COM CONTROLE DE PARTICIONAMENTO
    logger.info("\n💾 === GRAVAÇÃO DOS DADOS ===")
    gravador = Gravacao(spark, attributes)
    
    # Pegar configurações de particionamento
    num_particoes = attributes.get("partitioning", {}).get("num_particoes")
    particionar_por = attributes.get("partitioning", {}).get("particionar_por")
    
    gravador.gravar(
        df_enriquecido,
        num_particoes=num_particoes,
        particionar_por=particionar_por
    )

    # 8. MOVER ARQUIVOS PROCESSADOS
    move_files = attributes.get("processing", {}).get("move_processed_files", False)
    if move_files:
        logger.info("📦 Movendo arquivos processados...")
        processed_folder = attributes.get("processing", {}).get("processed_folder", "./data/processed")
        source_path = attributes.get("source", {}).get("file_path", "")
        
        # Criar pasta de processados se não existir
        os.makedirs(processed_folder, exist_ok=True)
        
        # Encontrar todos os arquivos que foram lidos
        arquivos = glob.glob(source_path)
        
        for arquivo in arquivos:
            nome_arquivo = os.path.basename(arquivo)
            destino = os.path.join(processed_folder, nome_arquivo)
            
            # Mover arquivo
            shutil.move(arquivo, destino)
            logger.info(f"  ✅ {nome_arquivo} movido para {destino}")
        
        logger.info(f"📦 {len(arquivos)} arquivo(s) movido(s) para {processed_folder}")

    logger.info("🎉 Pipeline concluído!")
    spark.stop()


if __name__ == "__main__":
    main()