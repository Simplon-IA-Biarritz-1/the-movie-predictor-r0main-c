# -*- coding:utf-8 -*-

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from bson.objectid import ObjectId

class DbConnect:
    '''Se connecter à la base de données et interagir avec elle'''

    def __init__(self, db, host='localhost', port=27017):
        self.client = MongoClient(host, port) # Informations de connexion au serveur MongoDB
        self.db_socket = self.client[db] # Information de connexion à "la bdd"

        # Vérifie que la connexion fonctionne
        try:
            self.db_socket.collection_names()
            print(f'Connexion à la base de données : {host}:{port} réussie!')
        except ServerSelectionTimeoutError:
            raise ServerSelectionTimeoutError(f'Echec de la connexion à la base de données : {host}:{port}!')

    def insert(self, collection, data, silent=False):
        '''Insère des documents dans une collection'''

        # Insère les documents dans la bdd
        post_ids = self.db_socket[collection].insert_many(data)

        # Par défaut affiche un message de réussite et les ids des documents insérés dans la collection
        if silent is not True:
            print(f'Documents insérés dans la collection : {collection} avec les ids : {str(post_ids.inserted_ids)}')

    def update(self, collection, data):
        '''Méthode de maj de la bdd a créer'''
        pass

    def get(self, collection, id_doc):
        '''Retoune un document contenu dans la bdd'''
        return self.db_socket[collection].find_one({"_id": ObjectId(id_doc)})

    def delete(self, collection, id_doc, silent=False):
        '''Supprime un document de la bdd'''
        self.db_socket[collection].delete_one({'_id':ObjectId(id_doc)})

        # Par défaut affiche un message de réussite et l'id du document supprimé
        if silent is not True:
            print(f'Document id : {id_doc} supprimé!')