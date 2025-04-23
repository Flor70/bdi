"""DAG Integrity Tests for S8 Project DAGs.

Ensures that all DAGs in the dags/ folder:
- Have defined tags.
- Have default retries configured (>= 1).
- Do not raise Python import errors.
"""

import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag


# Directory containing the DAGs relative to the project root or Airflow home
DAG_FOLDER = os.path.join(os.path.dirname(__file__), '..', '..', 'dags')
print(f"DAG FOLDER is: {DAG_FOLDER}")


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow.models.dagbag"):  # Be more specific
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)

        def strip_path_prefix(path):
            # Attempt to make path relative, fallback to original if error
            try:
                return os.path.relpath(path, DAG_FOLDER)
            except ValueError:
                return path

        # Ensure a test object is always created
        import_errors = [(None, None)]
        if dag_bag.import_errors:
            import_errors.extend([
                (strip_path_prefix(k), v.strip())
                for k, v in dag_bag.import_errors.items()
            ])
        return import_errors


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow.models.dagbag"):  # Be more specific
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)

    def strip_path_prefix(path):
        # Attempt to make path relative, fallback to original if error
        try:
            return os.path.relpath(path, DAG_FOLDER)
        except ValueError:
            return path

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


# Parameterize tests with DAG file locations for better reporting
dag_tuples = get_dags()
import_error_tuples = get_import_errors()


@pytest.mark.parametrize(
    "rel_path,rv", import_error_tuples, ids=[str(x[0]) for x in import_error_tuples]
)
def test_dag_import_errors(rel_path, rv):
    """Test for import errors in DAG files."""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


# We don't need a specific list for this project, just check tags exist
# APPROVED_TAGS = {}

@pytest.mark.parametrize(
    "dag_id,dag,fileloc", dag_tuples, ids=[x[2] for x in dag_tuples]
)
def test_dag_tags_defined(dag_id, dag, fileloc):
    """
    Test if all DAGs have tags defined.
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags defined."
    assert isinstance(dag.tags, list) and len(
        dag.tags) > 0, f"{dag_id} in {fileloc} tags should be a non-empty list."


@pytest.mark.parametrize(
    "dag_id,dag, fileloc", dag_tuples, ids=[x[2] for x in dag_tuples]
)
def test_dag_retries_configured(dag_id, dag, fileloc):
    """
    Test if all DAGs have default_args['retries'] configured (>= 1).
    """
    default_args = dag.default_args or {}
    retries = default_args.get("retries", None)
    assert retries is not None and isinstance(retries, int) and retries >= 1, \
        f"{dag_id} in {fileloc} must have default_args['retries'] set to an integer >= 1 (found: {retries})."

# You can add more DAG-level tests here if needed
# e.g., testing schedule_interval formats, ownership, etc.
