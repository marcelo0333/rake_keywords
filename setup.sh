#!/bin/bash

echo "Criando ambiente virtual..."
python -m venv venv

echo "Ativando ambiente..."
source venv/bin/activate

echo "Instalando dependências..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Download do NLTK..."
python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt')"

echo "Setup concluído!"