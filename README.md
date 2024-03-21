O projeto stone, tem como objetivo buscar dados da api do governo 
https://dadosabertos.rfb.gov.br/CNPJ/
Baixar dados do endpoint Empresa1 e Socio1
Gravar em uma camada "bronze" com dados brutos no formato .csv
Na segunda etapa, os dados brutos são tipados e gravados no formato parquet na camada silver.
Foi criada uma tabela com dados sumarizados dos socios por empresas. Esses arquivos também são disponibilizados em um banco de dados relacional mysql na amazon.
