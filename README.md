# exams-pepeline
# Prédiction des retards de vols avec PySpark et Airflow

## Introduction

En tant qu'analyste travaillant en collaboration avec une compagnie aérienne, notre mission consiste à examiner les données de vols existantes et à développer un modèle de données permettant de prédire si un vol sera ponctuel ou en retard.

## Prérequis

(Pyspark , pyhton airflow DAGs, linux(ubunu), vscode, airflow webserver , airflow  scheduler,pyspark-submit)

##la documentation
https://stackoverflow.com/questions/76861212/airflow-cannot-communicate-with-my-spark-master-to-launch-a-pyspark-job
https://stackoverflow.com/questions/72917746/spark-sesssion-in-airflow-dag
https://stackoverflow.com/questions/73903289/airflow-db-init-doesnt-create-an-expected-airflow-directory-on-windows
https://medium.com/international-school-of-ai-data-science/setting-up-apache-airflow-in-ubuntu-324cfcee1427

## Requierements
pyspark,python3,apache airflow)


## Decription du projet:
Description du Projet :

Ce projet vise à créer un flux de travail de prédiction des retards de vols en utilisant PySpark, un framework de traitement de données en mémoire, et Apache Airflow, un outil de gestion des workflows. L'objectif est de prédire les retards au départ et à l'arrivée des vols en fonction des données historiques des compagnies aériennes.

## Étapes du Projet :

Configuration de l'environnement : Mise en place de l'environnement PySpark et Apache Airflow pour le traitement des données et la gestion des tâches.

Chargement des Données : Les données historiques des aéroports, des vols et des données brutes des vols sont chargées à partir de fichiers CSV.

Nettoyage des Données : Les données sont nettoyées en supprimant les doublons et en éliminant les valeurs manquantes pour s'assurer de la qualité des données.

Transformation des Données : Les données sont transformées en ajoutant des colonnes numériques pour la prédiction, telles que la conversion de la compagnie aérienne en valeur numérique.

Préparation des Jeux de Données : Les données sont divisées en jeux de données d'entraînement et de test pour évaluer la performance du modèle.

Modélisation : Deux modèles de régression linéaire, l'un pour les retards au départ et l'autre pour les retards à l'arrivée, sont construits en utilisant PySpark MLlib.

Évaluation du Modèle : Les modèles sont évalués à l'aide de la racine carrée de l'erreur quadratique moyenne (RMSE) pour mesurer leur performance.

Suppression des Colonnes Inutiles : Les colonnes inutiles dans les prédictions sont supprimées pour créer un ensemble de prédictions finales.

Le calcul de la moyen du retard

la prédiction finale: si le vol aura du retard ou pas avec la classification binaire.

Création d'un DAG Airflow : Un workflow Airflow est créé pour planifier et exécuter le processus de prédiction de manière automatisée.

Exécution Automatisée : Le DAG Airflow est planifié pour exécuter régulièrement le processus de prédiction, garantissant que les modèles sont mis à jour avec de nouvelles données.

Surveillance et Rapports : Les résultats des prédictions sont surveillés et des rapports sont générés pour évaluer les performances du modèle au fil du temps.

Ce projet permettra aux compagnies aériennes d'anticiper et de gérer les retards de vols de manière plus efficace, améliorant ainsi l'expérience globale des voyageurs.


### Langages & Frameworks

python, spark hadoop, apache airfow

### Outils
linux ubuntu, vscode airflow webserver


#### Déploiement

Le déploiement d'un projet de prédiction des retards de vols avec PySpark et Apache Airflow peut être divisé en plusieurs étapes clés :

Préparation de l'environnement de déploiement : Vous devez préparer l'environnement de déploiement, y compris l'installation de PySpark, Apache Airflow, et toute autre dépendance requise sur votre serveur ou cluster.

Configuration d'Airflow : Configurez Apache Airflow en définissant des variables d'environnement, des connexions de base de données, des pools et des tâches, en fonction de votre architecture de déploiement.

Emballage de l'application PySpark : Vous devez emballer votre application PySpark, y compris le code, les fichiers de données, les modèles de machine learning, et tout autre fichier nécessaire.

Transfert vers le Serveur/Cluster : Transférez votre application emballée vers le serveur ou le cluster où Apache Airflow sera exécuté.

Définition du DAG Airflow : Définissez un DAG Airflow qui inclut toutes les étapes du processus de prédiction, y compris le chargement des données, le nettoyage, la transformation, la modélisation, l'évaluation, la génération de rapports, etc.

Planification des Tâches : Planifiez les tâches du DAG pour qu'elles s'exécutent à des intervalles réguliers, en fonction de vos besoins. Vous pouvez également activer des déclencheurs basés sur des événements spécifiques.

Configuration de la Surveillance : Configurez des alertes et des mécanismes de surveillance pour être informé en cas de dysfonctionnement du workflow.

Exécution du DAG : Activez le DAG dans Apache Airflow pour qu'il commence à s'exécuter selon le planning spécifié.

Surveillance en Temps Réel : Surveillez le statut des tâches et des workflows en temps réel à l'aide de l'interface utilisateur d'Apache Airflow.

Gestion des Erreurs : Mettez en place une gestion des erreurs pour traiter les problèmes éventuels, comme les échecs de tâches ou les erreurs dans le code PySpark.

Maintenance et Mises à Jour : Assurez la maintenance continue du système, en mettant à jour les modèles, les données, et les workflows selon les besoins.

Évolutivité : Si nécessaire, planifiez l'évolutivité du système en ajoutant des ressources matérielles ou en ajustant la configuration d'Airflow pour prendre en charge un plus grand volume de données.

Gestion des Accès : Assurez-vous que les autorisations d'accès aux données et aux ressources sont correctement gérées pour des questions de sécurité.

## Résultats

![features](https://github.com/Mawssi06/exams-pepeline/assets/118084452/867af799-37c1-4144-84d4-4e2362ff0506)

![predict_depp](https://github.com/Mawssi06/exams-pepeline/assets/118084452/81967ade-c518-4152-a571-ab0eadd58128)


![predict_arriv](https://github.com/Mawssi06/exams-pepeline/assets/118084452/5abc8c3c-3531-4ec3-ac02-3f6b9f8c9ad5)


![predict_tota](https://github.com/Mawssi06/exams-pepeline/assets/118084452/ed72e5e3-9fd4-40a5-aec6-752393c86eb3)

![retard ou non](https://github.com/Mawssi06/exams-pepeline/assets/118084452/6d0203db-6d61-479b-887b-6c4b53dde463)


## Etape Dags

![dags 1 taches](https://github.com/Mawssi06/exams-pepeline/assets/118084452/2f79aabd-30c0-4fea-823f-cbbc88297f41)
![dag pour 10 taches](https://github.com/Mawssi06/exams-pepeline/assets/118084452/9b49d6d6-6086-4e62-9a86-9cb58e0350d2)












