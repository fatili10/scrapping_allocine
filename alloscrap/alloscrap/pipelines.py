# # Define your item pipelines here
# #
# # Don't forget to add your pipeline to the ITEM_PIPELINES setting
# # See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# # useful for handling different item types with a single interface
# from itemadapter import ItemAdapter
# import csv
# import re

# # class CsvPipeline:
# #     def open_spider(self, spider):
# #         self.csv_file = open('data.csv', 'w', newline='', encoding='utf-8')
# #         self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=['title', 'original_title', 'score', 'genre', 'year', 'duration', 'description', 'actors', 'director'])
# #         self.csv_writer.writeheader()

# #     def close_spider(self, spider):
# #         self.csv_file.close()

# class AlocineScraperPipeline:
#     def process_item(self, item, spider):
#         # self.csv_writer.writerow(item)
#         item = self.duration_cleaned(item)
#         item = self.year_cleaned(item)
#         return item

#     # def clean_duree(self, item):
#     #     adapter = ItemAdapter(item)
#     #     duree = adapter.get('duration')
#     #     duration_cleaned = duree.strip().replace('/n', '')
#     #     adapter['duration'] = duration_cleaned
#     #     return item
    
#     # def duration_cleaned(self, item):
#     #     adapter = ItemAdapter(item)
#     #     duration = adapter.get('duration')
#     #     if duration:
#     #         duration_cleaned = ' '.join(duration).strip().replace('\n', '')
#     #         adapter['duration'] = duration_cleaned
#     #     return item
    
#     # def duration_cleaned(self, item):
#     #     adapter = ItemAdapter(item)
#     #     duration = adapter.get('duration')
#     #     if duration:
#     #         # Join the list into a single string, remove newlines, and split by commas
#     #         duration = ' '.join(duration).replace('\n', '').split(',')
#     #         # Strip whitespace from each part and filter out empty parts
#     #         duration_cleaned = ' '.join(part.strip() for part in duration if part.strip())
#     #         adapter['duration'] = duration_cleaned
#     #     return item
#     def duration_cleaned(self, item):
#         adapter = ItemAdapter(item)
#         duration = adapter.get('duration')
#         if duration:
#             duration = ' '.join(duration).replace('\n', '').split(',')
#             duration_cleaned = ' '.join(part.strip() for part in duration if part.strip())
            
#             # Extract hours and minutes from the cleaned duration string
#             hours = re.search(r'(\d+)\s*h', duration_cleaned)
#             minutes = re.search(r'(\d+)\s*min', duration_cleaned)

#             total_minutes = 0
#             if hours:
#                 total_minutes += int(hours.group(1)) * 60
#             if minutes:
#                 total_minutes += int(minutes.group(1))

#             adapter['duration'] = total_minutes
#         return item
    
#     def year_cleaned(self, item):
#         adapter = ItemAdapter(item)
#         year = adapter.get('year')
#         if year:
#             # Remove any newline characters and extra spaces
#             year_cleaned = year.strip()
#             # Extract the year using regex
#             match = re.search(r'\b\d{4}\b', year_cleaned)
#             if match:
#                 year_cleaned = match.group(0)
#             else:
#                 year_cleaned = ""
#             adapter['year'] = year_cleaned
#         return item
################################################################
#################################################################
import os
import re
from itemadapter import ItemAdapter
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # Charger les variables d'environnement depuis le fichier .env

class AlocineScraperPipeline:
    def process_item(self, item, spider):
        item = self.duration_cleaned(item)
        item = self.year_cleaned(item)
        item = self.score_cleaned(item)  # Nouvelle méthode pour nettoyer le score
        return item


    def score_cleaned(self, item):
        adapter = ItemAdapter(item)
        score = adapter.get('score')
        if score:
            # Remplacer la virgule par un point pour la conversion en float
            score_cleaned = score.replace(',', '.')  
            adapter['score'] = float(score_cleaned)
        return item
    
    def duration_cleaned(self, item):
        adapter = ItemAdapter(item)
        duration = adapter.get('duration')
        if duration:
            duration = ' '.join(duration).replace('\n', '').split(',')
            duration_cleaned = ' '.join(part.strip() for part in duration if part.strip())

            hours = re.search(r'(\d+)\s*h', duration_cleaned)
            minutes = re.search(r'(\d+)\s*min', duration_cleaned)

            total_minutes = 0
            if hours:
                total_minutes += int(hours.group(1)) * 60
            if minutes:
                total_minutes += int(minutes.group(1))

            adapter['duration'] = total_minutes
        return item
    
    def year_cleaned(self, item):
        adapter = ItemAdapter(item)
        year = adapter.get('year')
        if year:
            year_cleaned = year.strip()
            match = re.search(r'\b\d{4}\b', year_cleaned)
            if match:
                year_cleaned = match.group(0)
            else:
                year_cleaned = ""
            adapter['year'] = year_cleaned
        return item


class DatabasePipeline:
    def open_spider(self, spider):
        self.connection = psycopg2.connect(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME')
        )
        self.cursor = self.connection.cursor()
        self.create_tables()

    def create_tables(self):
        # Supprime les tables si elles existent déjà
        self.cursor.execute('''
            DROP TABLE IF EXISTS film_actor;
            DROP TABLE IF EXISTS film_director;
            DROP TABLE IF EXISTS films;
            DROP TABLE IF EXISTS actors;
            DROP TABLE IF EXISTS directors;
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS films (
                id SERIAL PRIMARY KEY,
                title TEXT,
                original_title TEXT,
                score FLOAT,
                genre TEXT,
                year INTEGER,
                duration INTEGER,
                description TEXT
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS actors (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE  -- Add UNIQUE constraint
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS directors (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE  -- Add UNIQUE constraint
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS film_actor (
                film_id INTEGER REFERENCES films(id),
                actor_id INTEGER REFERENCES actors(id),
                PRIMARY KEY (film_id, actor_id)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS film_director (
                film_id INTEGER REFERENCES films(id),
                director_id INTEGER REFERENCES directors(id),
                PRIMARY KEY (film_id, director_id)
            )
        ''')
        self.connection.commit()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        try:
            self.cursor.execute('''
                INSERT INTO films (title, original_title, score, genre, year, duration, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                adapter.get('title'),
                adapter.get('original_title'),
                float(adapter.get('score')) if adapter.get('score') else None,
                adapter.get('genre'),
                int(adapter.get('year')) if adapter.get('year') else None,
                adapter.get('duration'),
                adapter.get('description')
            ))
            film_id = self.cursor.fetchone()[0]

            # Insert actors
            for actor in adapter.get('actors', []):
                self.cursor.execute('''
                    INSERT INTO actors (name) VALUES (%s)
                    ON CONFLICT (name) DO NOTHING  -- Ensure name is unique
                    RETURNING id
                ''', (actor,))
                actor_id = self.cursor.fetchone()
                if actor_id:
                    actor_id = actor_id[0]
                else:
                    self.cursor.execute('SELECT id FROM actors WHERE name = %s', (actor,))
                    actor_id = self.cursor.fetchone()[0]

                self.cursor.execute('''
                    INSERT INTO film_actor (film_id, actor_id) VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                ''', (film_id, actor_id))

            # Insert directors
            for director in adapter.get('director', []):
                self.cursor.execute('''
                    INSERT INTO directors (name) VALUES (%s)
                    ON CONFLICT (name) DO NOTHING  -- Ensure name is unique
                    RETURNING id
                ''', (director,))
                director_id = self.cursor.fetchone()
                if director_id:
                    director_id = director_id[0]
                else:
                    self.cursor.execute('SELECT id FROM directors WHERE name = %s', (director,))
                    director_id = self.cursor.fetchone()[0]

                self.cursor.execute('''
                    INSERT INTO film_director (film_id, director_id) VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                ''', (film_id, director_id))

            self.connection.commit()

        except psycopg2.Error as e:
            self.connection.rollback()  # Rollback the transaction on error
            spider.log(f"Error executing SQL: {e}")
            raise  # Re-raise the exception to indicate failure

        return item
