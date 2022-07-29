from metaflow import FlowSpec, step, IncludeFile, Parameter
from metaflow import pyspark
import myjob

class PySparkTestFlow(FlowSpec):

    param = Parameter('param', default=123)
    spark_config = IncludeFile('spark_config', default='spark_config.json')

    @pyspark(job=myjob.run,
             job_parameters=['param'],
             output_artifact='spark_df',
             output_pandas=True)
    @step
    def start(self):
        print('PySpark job produced %d rows' % len(self.spark_df))
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    PySparkTestFlow()
