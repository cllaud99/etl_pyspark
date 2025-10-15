# 🧩 Módulo de Transformações PySpark

Este módulo define a classe `Transformacao`, que centraliza **boas práticas de limpeza e padronização de dados** usando **PySpark**.  
Permite encadear transformações de forma simples e legível.

## 🚀 Funcionalidades

- **converter_tipo** → Converte o tipo de uma coluna (`string` → `integer`, `date`, etc.)
- **normaliza_texto_simples** → Remove espaços extras e converte texto para minúsculas
- **normaliza_nomes** → Formata nomes em _Title Case_
- **normaliza_nomes_remove_acentuacao** → Remove acentuação e formata em _Title Case_
- **formatar_datas** → Converte formatos de datas (ex: `"dd/MM/yyyy"` → `"yyyy-MM-dd"`)