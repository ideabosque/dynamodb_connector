#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import boto3
from boto3.dynamodb.conditions import Key

datetime_format = "%Y-%m-%dT%H:%M:%S%z"


class DynamoDBConnector(object):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting

    def connect(self):
        if (
            self.setting.get("region_name")
            and self.setting.get("aws_access_key_id")
            and self.setting.get("aws_secret_access_key")
        ):
            return boto3.resource(
                "dynamodb",
                region_name=self.setting.get("region_name"),
                aws_access_key_id=self.setting.get("aws_access_key_id"),
                aws_secret_access_key=self.setting.get("aws_secret_access_key"),
            )
        else:
            return boto3.resource("dynamodb")

    @property
    def dynamodb(self):
        return self.connect()

    def get_count(self, source, updated_at_from, updated_at_to, table_name=None):
        table = self.dynamodb.Table(table_name)
        updated_at_from = updated_at_from.strftime(datetime_format)
        updated_at_to = updated_at_to.strftime(datetime_format)
        response = table.query(
            IndexName="updated_at-index",
            KeyConditionExpression=Key("source").eq(source)
            & Key("updated_at").between(updated_at_from, updated_at_to),
        )
        count = response["Count"]
        while "LastEvaluatedKey" in response:
            response = table.query(
                IndexName="updated_at-index",
                KeyConditionExpression=Key("source").eq(source)
                & Key("updated_at").between(updated_at_from, updated_at_to),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            count = count + response["Count"]
        return count

    def get_items(
        self,
        source,
        updated_at_from,
        updated_at_to,
        table_name=None,
        limit=100,
        offset=0,
    ):
        table = self.dynamodb.Table(table_name)
        updated_at_from = updated_at_from.strftime(datetime_format)
        updated_at_to = updated_at_to.strftime(datetime_format)
        response = table.query(
            IndexName="updated_at-index",
            KeyConditionExpression=Key("source").eq(source)
            & Key("updated_at").between(updated_at_from, updated_at_to),
        )
        items = response["Items"]
        while "LastEvaluatedKey" in response:
            response = table.query(
                IndexName="updated_at-index",
                KeyConditionExpression=Key("source").eq(source)
                & Key("updated_at").between(updated_at_from, updated_at_to),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response["Items"])

        if len(items) == 0:
            return []

        items = sorted(items, key=lambda x: x["updated_at"], reverse=False)
        return items[offset : int(offset) + int(limit)]

    def get_items_by_key(self, source, value, table_name=None, key=None):
        table = self.dynamodb.Table(table_name)

        response = table.query(
            IndexName=f"{key}-index",
            KeyConditionExpression=Key("source").eq(source)
            & Key(key).begins_with(value),
        )

        if response["Count"] == 0:
            return []

        return response["Items"]

    def get_item(self, source, value, table_name=None, key=None):
        table = self.dynamodb.Table(table_name)

        response = table.query(
            IndexName=f"{key}-index",
            KeyConditionExpression=Key("source").eq(source) & Key(key).eq(value),
        )

        if response["Count"] == 0:
            return None

        if response["Count"] == 1:
            return response["Items"][0]

        last_item = max(response["Items"], key=lambda item: item["updated_at"])
        for duplicated_item in list(
            filter(lambda x: (x["id"] != last_item["id"]), response["Items"])
        ):
            table.delete_item(Key={"id": duplicated_item["id"]})
        return last_item

    def put_item(self, entity, table_name=None):
        self.dynamodb.Table(table_name).put_item(Item=entity)
        return
