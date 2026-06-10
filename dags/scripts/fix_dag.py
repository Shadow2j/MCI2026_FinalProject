from airflow.models import DagRun
from airflow.utils.session import create_session

with create_session() as session:
    drs = session.query(DagRun).filter(DagRun.dag_id == 'dustinia_cx_pipeline').all()
    count = 0
    for dr in drs:
        if dr.state in ['queued', 'running', 'failed']:
            dr.state = 'success'
            count += 1
    session.commit()
    print(f'Marked {count} DAG runs as success.')
