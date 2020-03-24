# -*- coding:utf-8 -*-

from tsv import TsvReader
from db import DbConnect
import glob
import os
import subprocess
from tqdm import tqdm

# Scan du dossier data contenant les fichiers csv
tsv_files = glob.glob('data/*.tsv')

# Instancie le "connecteur" à la bdd
db_remote = DbConnect('TheMoviesPredictor')

# Scan des fichiers csv
for file in tsv_files:

    # Instancie le "lecteur séquentiel de fichier"
    tsv_reader = TsvReader(f'{file}', nb_lines=100)

    # Récupère le nombre de lignes totales du fichier csv en cours
    total_lines = int(subprocess.Popen(f'wc/wc.exe -l {file}', stdout=subprocess.PIPE).stdout.read().decode('utf-8').split()[0])+1

    # Récupère le nom du fichier (juste le nom pas le chemin complet...)
    file_name = os.path.basename(file)

    # Initialise la barre de progression
    pbar = tqdm(total_lines)
    os.system('cls')

    print(f'Insertion de : {file_name}\n')

    while True:
        # Lecture séquentielle
        data = tsv_reader.read_sequential()

        # Stop la lecture si le fichier a été parcouru entièrement
        if data is None:
            break

        # Conversion des types de données
        data_convert = list()

        for doc in data:
            doc_convert = dict()

            for cle, val in doc.items():
                if cle in ['endYear', 'isAdult', 'runtimeMinutes', 'startYear']:
                    try:
                        val_convert = int(val)
                    except ValueError:
                        val_convert = 0

                    doc_convert[cle] = val_convert

                elif cle == 'genres':
                    genres = val.split(',')

                    if len(genres) > 1:
                        for nb, genre in enumerate(genres):
                            doc_convert[f'genre_{nb+1}'] = genre
                    else:
                        doc_convert['genres'] = val

                else:
                    doc_convert[cle] = val

            data_convert.append(doc_convert)

        data = list(data_convert)

        # Insére les lignes lues dans la bdd
        db_remote.insert(file_name[:file_name.index('.')], data, silent=True)

        # MAJ de la barre de de progression
        pbar.update(tsv_reader.nb_lines)

    pbar.close()

print('\nImportation des fichiers terminée!\n')
os.system('pause')