#encoding=utf-8
import sys
from batchcompute import Client, ClientError
from batchcompute import CN_BEIJING as REGION
from batchcompute.resources import (
    JobDescription, TaskDescription, DAG, AutoCluster
)

ACCESS_KEY_ID='' # 填写您的AK
ACCESS_KEY_SECRET='' # 填写您的AK
IMAGE_ID = 'img-f5tcaqrmrllg4sjgngo00m' #这里填写您的镜像ID
INSTANCE_TYPE = 'ecs.sn1.medium' # 根据实际region支持的InstanceType 填写 ecs.s1.medium  ecs.sn1ne.2xlarge
LOG_PATH = 'oss://dongyu1009/blocking/logs/' # 'oss://your-bucket/log-count/logs/' 这里填写您创建的错误反馈和task输出的OSS存储路径
INPUT_MOUNT= 'oss://dongyu1009/blocking/input/' 
OUTPUT_MOUNT= 'oss://dongyu1009/blocking/temp2/'
client = Client(REGION, ACCESS_KEY_ID, ACCESS_KEY_SECRET)
def main(year):
    WORKER_PATH = 'oss://dongyu1009/blocking/program3.tar.gz' # 'oss://your-bucket/log-count/log-count.tar.gz'  这里填写您上传的log-count.tar.gz的OSS存储路径
    try:
        job_desc = JobDescription()
        # Create split task.
        compute_task = TaskDescription()
        compute_task.AutoCluster.Configs.Networks.VPC.CidrBlock = "192.168.0.0/16"
        compute_task.AutoCluster.InstanceType = INSTANCE_TYPE
        compute_task.AutoCluster.ResourceType = "OnDemand"
        compute_task.AutoCluster.ImageId = IMAGE_ID
        # compute_task.AutoCluster.Configs.Networks.VPC.VpcId = "vpc-xxyyzz"  # 如果想要使用用户VPC功能，需要设置此字段
        compute_task.Parameters.Command.CommandLine = "python lucc_combine.py"
        compute_task.Parameters.Command.PackagePath = WORKER_PATH
        compute_task.Parameters.StdoutRedirectPath = LOG_PATH
        compute_task.Parameters.StderrRedirectPath = LOG_PATH
        compute_task.InstanceCount = 1
        compute_task.Timeout = 36000
        # compute_task.AutoCluster = cluster
        # compute_task.AutoCluster.ReserveOnFail = True # 失败时保留现场
        compute_task.InputMapping[INPUT_MOUNT]='D:'
        compute_task.OutputMapping['C:\\\\output\\'] = OUTPUT_MOUNT
        # Create task dag.
        task_dag = DAG()
        task_dag.add_task(task_name="compute", task=compute_task)
        # Create job description.
        job_desc.DAG = task_dag
        job_desc.Priority = 999 # 0-1000
        job_desc.Name = "block_compute_" + year
        job_desc.Description = "block_compute_formal"
        job_desc.JobFailOnInstanceFail = True
        job_id = client.create_job(job_desc).Id
        print('job created: %s' % job_id)
    except ClientError, e:
        print (e.get_status_code(), e.get_code(), e.get_requestid(), e.get_msg())

def runall(years):
    for i in range(len(years)):
        year = years[i]
        main(year)
        
if __name__ == '__main__':
    # years = ['1980','1995','2000','2010','2015']
    years = ['1980']
    sys.exit(runall(years))
