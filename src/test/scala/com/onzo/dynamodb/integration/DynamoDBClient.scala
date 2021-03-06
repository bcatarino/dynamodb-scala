/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.onzo.dynamodb.integration

import scala.concurrent.ExecutionContext.Implicits.global

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._
import com.github.dwhjames.awswrap.dynamodb.AmazonDynamoDBScalaClient
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.sys.process.Process
;


trait DynamoDBClient
  extends BeforeAndAfterAll
     with AwaitHelper
{ self: Suite =>

  private val logger: Logger = LoggerFactory.getLogger(self.getClass)

  val client = {
    val jClient = new AmazonDynamoDBAsyncClient(new BasicAWSCredentials("FAKE_ACCESS_KEY", "FAKE_SECRET_KEY"))
    jClient.setEndpoint("http://localhost:8000")

    new AmazonDynamoDBScalaClient(jClient)
  }

  val tableNames: Seq[String]

  override def beforeAll(): Unit = {
    Process("./start-dynamodb-local.sh").!

    deleteAllSpecifiedTables()

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      deleteAllSpecifiedTables()
    }
    Process("./stop-dynamodb-local.sh").!
  }

  private def deleteAllSpecifiedTables(): Unit = {
    tableNames foreach tryDeleteTable

    tableNames foreach awaitTableDeletion
  }

  def tryDeleteTable(tableName: String): Unit = {
    logger.info(s"Deleting $tableName table")
    await {
      client.deleteTable(tableName) recover { case e: ResourceNotFoundException => () }
    }
    ()
  }

  def awaitTableDeletion(tableName: String): Unit = {
    logger.info(s"Waiting for $tableName table to be deleted.")

    val deadline = 10.minutes.fromNow

    // Don't know the api, but this whole block of code is very non-functional and non-reactive. client.describeTable
    // returns a future, so instead of a while and waiting for a result, why not map the future so that on success.
    // Besides, using Thread.sleep in a test is definitely not a good idea. The thread will not be doing any work
    // for 20 seconds and it will prevent other tests from running (assuming they are running in parallel). It will
    // stall the CI environment more than necessary.
    while (deadline.hasTimeLeft) {
      try {
        val result = await {
          client.describeTable(tableName)
        }

        if (result.getTable.getTableStatus == TableStatus.ACTIVE.toString) return ()
        Thread.sleep(20 * 1000)
      } catch {
        case e: ResourceNotFoundException =>
          return ()
      }
    }
    throw new RuntimeException(s"Timed out waiting for $tableName table to be deleted.")
  }

  def tryCreateTable(createTableRequest: CreateTableRequest): Unit = {
    logger.info(s"Creating ${createTableRequest.getTableName()} table")
    await {
      client.createTable(createTableRequest)
    }
    ()
  }

  def awaitTableCreation(tableName: String): TableDescription = {
      logger.info(s"Waiting for $tableName table to become active.")

      val deadline = 10.minutes.fromNow

      while (deadline.hasTimeLeft) {
        val result = await {
          client.describeTable(tableName)
        }

        // Same as above. This should be handled in a more functional way. No reason why a while or a return need to
        // exist here.
        val description = result.getTable
        if (description.getTableStatus == TableStatus.ACTIVE.toString)
          return description

        Thread.sleep(20 * 1000)
      }
      throw new RuntimeException(s"Timed out waiting for $tableName table to become active.")
    }

}
