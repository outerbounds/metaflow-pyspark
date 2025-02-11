from metaflow import FlowSpec, step, IncludeFile, Parameter, card
from metaflow import pyspark
import job_create, job_write_branch, job_audit, job_publish

jobconf = {
    'output_artifact': 'df',
    'output_pandas': True,
    'job_parameters': ['db', 'table']
}

class SparkWAPFlow(FlowSpec):

    db = Parameter('db', default='iceberg_demo_db')
    table = Parameter('table', default='iceberg_table')
    spark_config = IncludeFile('spark_config', default='spark_config.json')

    @pyspark(job=job_create.run, **jobconf)
    @card
    @step
    def start(self):
        self.next(self.write_branch)

    @pyspark(job=job_write_branch.run, **jobconf)
    @card
    @step
    def write_branch(self):
        self.next(self.audit)

    @pyspark(job=job_audit.run, **jobconf)
    @card
    @step
    def audit(self):
        self.next(self.publish)

    @pyspark(job=job_publish.run, **jobconf)
    @card
    @step
    def publish(self):
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    SparkWAPFlow()
