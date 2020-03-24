# -*- coding:utf-8 -*-

import os

class TsvReader:
    '''Lire de manière séquentielle les fichiers .tsv à importer dans la base données'''

    def __init__(self, file_path, nb_lines=1):

        # Vérifie que le chemin du fichier est correct, si oui l'enregistre dans l'attribut file de l'objet
        if os.path.exists(file_path):
            self.file = open(file_path, encoding='utf-8')
        else:
            raise FileNotFoundError(f'Le fichier {file_path} n\'existe pas!')

        # Nombre de lignes à lire à chaque lecture du fichier
        self.nb_lines = nb_lines

        # Récupère l'entête du fichier (les colonnes) pour créer les dictionnaires qui permettront d'insérer les lignes dans la bdd
        self.headers = self.file.readlines(1)[0].strip().split('\t')

    def read_sequential(self):

        lines = list()
        output = list()
        counter = self.nb_lines

        # Tant que counter (égal au nombre de lignes à lire en une fois dans le fichier) n'est pas égal à 0 : lire une par une les lignes du fichier
        while counter > 0:
            line = self.file.readlines(1) # Démarre la lecture sur la ligne 1 puisque la ligne 0 a déjà été lue et correspond à l'entête du fichier

            # Si la ligne n'est pas vide, supprimer les éventuels espaces à la fin et l'ajouter à la liste des lignes du fichier à retourner
            if line != []:
                lines.append(line[0].strip())
                counter -= 1

            # Si la ligne est vide c'est que l'on a atteint la fin du fichier dans ce cas on stop de suite la lecture
            else:
                break

        # Si des lignes on été retournées pendant la lecture du fichier
        if lines != []:
            for l in lines:

                # Splits de la ligne afin d'avoir dans une liste les valeurs des colonnes (colonnes du headers)
                line_content = l.split('\t')

                # Créer un dictionnaire avec cette structure : {'colonne1':valeur1, 'colonne2':valeur2}
                tmp_dict = dict()
                for index, content in enumerate(self.headers):
                    tmp_dict[content] = line_content[index]

                # Ajoute le dictionnaire contenant la ligne (ansi que ces colonnes...) à la liste des lignes à retourner pour écriture dans la bdd
                output.append(tmp_dict)

            return output