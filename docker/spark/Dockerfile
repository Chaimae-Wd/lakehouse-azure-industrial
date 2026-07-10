FROM apache/spark:3.5.6

USER root

# Création des dossiers utilisés par le projet et par Ivy.
# Ivy est le gestionnaire utilisé par Spark pour télécharger les packages Maven.
RUN mkdir -p /opt/spark/work-dir \
    && mkdir -p /opt/spark-data/bronze \
    && mkdir -p /home/spark/.ivy2/cache \
    && mkdir -p /home/spark/.ivy2/jars \
    && chown -R spark:spark /opt/spark/work-dir \
    && chown -R spark:spark /opt/spark-data \
    && chown -R spark:spark /home/spark/.ivy2

USER spark

WORKDIR /opt/spark/work-dir