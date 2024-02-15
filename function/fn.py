"""A Crossplane composition function."""

import grpc
from crossplane.function import logging, response, resource
from crossplane.function.proto.v1beta1 import run_function_pb2 as fnv1beta1
from crossplane.function.proto.v1beta1 import run_function_pb2_grpc as grpcv1beta1

# from google.protobuf import json_format

class FunctionRunner(grpcv1beta1.FunctionRunnerService):
    """A FunctionRunner handles gRPC RunFunctionRequests."""

    def __init__(self):
        """Create a new FunctionRunner."""
        self.log = logging.get_logger()

    async def RunFunction(
        self, req: fnv1beta1.RunFunctionRequest, _: grpc.aio.ServicerContext
    ) -> fnv1beta1.RunFunctionResponse:
        """Run the function."""
        log = self.log.bind(tag=req.meta.tag)
        log.info("Running function")

        rsp = response.to(req)

        tgw_mode = ""
        
        if "extras" in req.input and "tgwMode" in req.input["extras"]:
            tgw_mode = req.input["extras"]["tgwMode"]

        log.info("tgwMode", tgw_mode=tgw_mode)

        # desired = resource.struct_to_dict(req.desired.resources)
        # print(desired)
        # s3b = desired["s3Bucket"]
        # print(s3b)
        # s3bd = json_format.MessageToDict(s3b)
        # print(s3bd)

        # bucket = req.desired.resources["s3Bucket"]
        # print(bucket)

        if tgw_mode == "ABC":
            newS3Bucket = {
                'kind': 'Bucket',
                'metadata': {},
                'spec': {
                    'providerConfigRef': {
                        'name': 'default'
                    },
                    'forProvider': {
                        'region': 'us-east-1'
                    }
                },
                'apiVersion': 's3.aws.upbound.io/v1beta1'
            }
            log.debug("creating new bucket", newBucket=newS3Bucket)
            newS3Struct = resource.dict_to_struct(newS3Bucket)
            newResource = fnv1beta1.Resource(
                resource=newS3Struct,
            )

            req.desired.resources.get_or_create("new1").CopyFrom(newResource)
            

            rsp.desired.resources.MergeFrom(req.desired.resources)

        # TODO: Add your function logic here!
        response.normal(rsp, f"I was run with input {tgw_mode}!")
        log.info("I was run!", input=tgw_mode)

        return rsp

class AnotherFunctionRunner(grpcv1beta1.FunctionRunnerService):
    """A FunctionRunner handles gRPC RunFunctionRequests."""

    def __init__(self):
        """Create a new FunctionRunner."""
        self.log = logging.get_logger()

    async def RunFunction(
        self, req: fnv1beta1.RunFunctionRequest, _: grpc.aio.ServicerContext
    ) -> fnv1beta1.RunFunctionResponse:
        """Run the function."""
        log = self.log.bind(tag=req.meta.tag)
        log.info("Running function")

        rsp = response.to(req)

        tgw_mode = ""
        
        if "extras" in req.input and "tgwMode" in req.input["extras"]:
            tgw_mode = req.input["extras"]["tgwMode"]

        log.info("tgwMode", tgw_mode=tgw_mode)

        # desired = resource.struct_to_dict(req.desired.resources)
        # print(desired)
        # s3b = desired["s3Bucket"]
        # print(s3b)
        # s3bd = json_format.MessageToDict(s3b)
        # print(s3bd)

        # bucket = req.desired.resources["s3Bucket"]
        # print(bucket)

        if tgw_mode == "ABC":
            newDynamo = {
                'kind': 'Table',
                'metadata': {},
                'spec': {
                    'providerConfigRef': {
                        'name': 'default'
                    },
                    'forProvider': {
                        'attribute': [
                            {'name': 'S3ID', 'type': 'S'}
                        ],
                        'hashKey': 'S3ID',
                        'readCapacity': 1,
                        'region': 'us-east-1',
                        'writeCapacity': 1,
                    }
                },
                'apiVersion': 'dynamodb.aws.upbound.io/v1beta1'
            }
            log.debug("creating new table", newDynamo=newDynamo)
            newDynamoStruct = resource.dict_to_struct(newDynamo)
            newResource = fnv1beta1.Resource(
                resource=newDynamoStruct,
            )

            req.desired.resources.get_or_create("newtable").CopyFrom(newResource)
            

            rsp.desired.resources.MergeFrom(req.desired.resources)

        # TODO: Add your function logic here!
        response.normal(rsp, f"I was run with input {tgw_mode}!")
        log.info("I was run!", input=tgw_mode)

        return rsp
