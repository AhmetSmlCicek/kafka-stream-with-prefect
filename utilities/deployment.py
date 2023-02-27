#Instead of running termianl command to deploy, you can run this python script

from kafkatest import boss
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=boss,
    name="kafka-train",
    work_queue_name="kafka2",
)

if __name__ == "__main__":
    deployment.apply()