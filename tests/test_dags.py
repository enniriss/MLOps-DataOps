from airflow.models import DagBag  

# 1. Charger les DAGs
def charger_dags():
    return DagBag(
        dag_folder="dags",
        include_examples=False,
    )


# 2. Vérifier qu'il n'existe aucune erreur d'import
def test_aucune_erreur_import():
    dag_bag = charger_dags()

    assert dag_bag.import_errors == {}, (
        f"Erreurs d'import : {dag_bag.import_errors}"
    )


# 3. Vérifier que le DAG user_processing existe
def test_user_processing_existe():
    dag_bag = charger_dags()

    assert "user_processing" in dag_bag.dags


# 4. Vérifier les tâches du DAG
def test_user_processing_taches():
    dag = charger_dags().dags["user_processing"]

    taches = {
        t.task_id
        for t in dag.tasks
    }

    attendues = {
        "create_table",
        "is_api_available",
        "extract_user",
        "process_user",
        "store_user",
    }

    # Vérifier que les tâches réelles correspondent aux tâches attendues
    assert taches == attendues, (
        f"Tâches incorrectes.\n"
        f"Tâches attendues : {attendues}\n"
        f"Tâches trouvées : {taches}"
    )
