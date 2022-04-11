# Booking_NLP
Projet architecture distribuée_ NLP sur des reviews d'hotel Booking


Ce git contient les codes qui nous ont permis de realiser le Projet d'analyse de sentiments en streaming sur des reviews d'hotel issues du site Booking.com
On y trouve dans l ordre d'utilisation : 
  1) La presentation du projet
  2) le Jupyter Notebook permetant de spliter les données en 2 fichiers ( 1 pour entrainer notre modele, 1 pour simuler l envoie de données via une api). 
Un preprocessing est egalement appliquer pour mettre en forme les données, corriger les erreurs et equilibrer les opinions (positives et negatives)
  3) le Producer (qui permet d envoyer les données en streaming vers notre modèle spark)
Dans le dossier Doker:
  4)  le docker-compose 
Dans le dossier Work
  5) Le Jupyter concernant l'optimisation du pipeline, le choix du meilleur modèle et l entrainement de ce dernier (Predict_booking_review)
  6) Le consumer, script appliquant le modele entrainé sur des reviews collectées à la volée. Les predictions sont stockées dans MondoDB : Booking.Reviews
(fichier Polarity _prediction _Streaming)
  7) Un jupiter permettant de se connecter a MongoDB pour collecter et travailler les données precedement stockées ( Mongo_exploration)
  8) On retrouve egalement les pipeline et Modèle sauvegardé dans Spark.
