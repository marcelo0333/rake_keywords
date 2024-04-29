import yake
from rake_nltk import Rake
import pandas as pd
import psycopg2
import time
import nltk
nltk.download('stopwords')
import nltk
nltk.download('punkt')

connection = psycopg2.connect(user="postgres",
                              password="1234",
                              host="localhost",
                              port="5433",
                              database="eventsDB")

# r = Rake(language="portuguese")
language = "pt"
max_ngra_size = 1
duplication_threshold = 0.7
numOfKeywords = 20

yakekeywordextractor = yake.KeywordExtractor(lan= language, n = max_ngra_size, dedupLim= duplication_threshold, top = numOfKeywords, features= None)

def extract_and_store_keyword(description, event_id):
    keywordsfromyake = yakekeywordextractor.extract_keywords(description)
    keywords = [keyword for score, keyword in keywordsfromyake]

    with connection.cursor() as cursor:
        for keyword, _ in keywordsfromyake:  # Ajuste aqui
            # Verificar se a palavra-chave já existe na tabela keywords
            cursor.execute("SELECT keyword_id FROM keywords WHERE name_keyword = %s", (keyword,))
            existing_keyword = cursor.fetchone()
            if existing_keyword:
                keyword_id = existing_keyword[0]
            else:
                # Inserir a palavra-chave na tabela keywords
                sql = "INSERT INTO keywords (name_keyword) VALUES (%s) RETURNING keyword_id"
                cursor.execute(sql, (keyword,))
                keyword_id = cursor.fetchone()[0]

            # Verificar se o par já existe na tabela events_to_keywords
            cursor.execute("SELECT 1 FROM events_to_keywords WHERE event_id = %s AND keyword_id = %s", (event_id, keyword_id))
            existing_entry = cursor.fetchone()
            if not existing_entry:
                # Relacionar o evento à palavra-chave na tabela events_to_keywords
                sql = "INSERT INTO events_to_keywords (event_id, keyword_id) VALUES (%s, %s)"
                cursor.execute(sql, (event_id, keyword_id))
                print(f"Evento {event_id} relacionado à palavra-chave '{keyword}'")
            else:
                print(f"Já existe uma entrada para o evento {event_id} e a palavra-chave '{keyword}'")

    connection.commit()

def get_events_from_database():
    with connection.cursor() as cursor:
        sql = "SELECT event_id, description FROM events"
        cursor.execute(sql)
        events = [{'event_id': row[0], 'description': row[1]} for row in cursor.fetchall()]
        return events

    
def extract_and_store_keywords_for_events():
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS keywords (
                keyword_id SERIAL PRIMARY KEY,
                name_keyword VARCHAR(255) UNIQUE
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events_to_keywords (
                event_id INT REFERENCES events(event_id),
                keyword_id INT REFERENCES keywords(keyword_id),
                PRIMARY KEY (event_id, keyword_id)
            );
        """)
    connection.commit()
    events = get_events_from_database()
    print("Eventos recuperados:", events)

    for event in events:
        event_id = event['event_id']
        description = event['description']

        if description:
            extract_and_store_keyword(description, event_id)

   

extract_and_store_keywords_for_events()

connection.close()

