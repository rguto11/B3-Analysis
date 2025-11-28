# Use a imagem base oficial do Python slim
FROM python:3.9-slim

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /home/site/wwwroot

# Copiar o arquivo requirements.txt e instalar as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o script principal
COPY function_app.py .

# O comando de execução será definido no Azure, mas o ENTRYPOINT é uma boa prática
# ENTRYPOINT ["python", "/home/site/wwwroot/function_app.py"]

# O Container App usará o comando de substituição que corrigimos:
# Comando: /usr/local/bin/python
# Argumento: /home/site/wwwroot/function_app.py