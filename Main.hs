{-# LANGUAGE OverloadedStrings #-}

import           Database.MySQL.Simple
import           Haskakafka
import           Haskakafka.InternalRdKafkaEnum
import qualified Control.Exception                  as E
import Control.Monad
import qualified Data.ByteString.Lazy               as BL
import qualified Data.ByteString.Char8              as C8

import Data.Aeson

type JobId = Int
type TransferMessage = JobId


getConnection :: IO Connection
getConnection = connect defaultConnectInfo {  connectHost = "localhost",
                                              connectPassword = "password",
                                              connectDatabase = "plads" }

makeanio :: TransferMessage -> IO () --TODO: call api, update status, send to mediaOutput if completed
makeanio t = print t

decodeTransferMessage :: KafkaMessage -> TransferMessage
decodeTransferMessage message = case C8.readInt $ messagePayload message of
  Nothing -> error ("decode failed: " ++ show (messagePayload message))
  Just (i, _) -> i

handleKafkaError :: KafkaError -> String
handleKafkaError (KafkaResponseError RdKafkaRespErrTimedOut) = "[INFO] " ++ show RdKafkaRespErrTimedOut
handleKafkaError e = "[ERROR] " ++ show e

handleKafkaMessage :: (KafkaMessage -> IO a) -> KafkaMessage -> IO ()
handleKafkaMessage process message = do
    print $ BL.fromStrict $ messagePayload message
    handled <- E.try(process message)
    case handled of
      Left e -> putStrLn $ "[ERROR]: " ++ show (e :: E.SomeException)
      Right io -> print "handled successfully"

handleConsume :: Show a => Either KafkaError KafkaMessage -> (KafkaError -> a) -> (KafkaMessage -> IO ()) -> IO ()
handleConsume (Left err) errorHandler _ = print $ errorHandler err
handleConsume (Right m) _ messageHandler = messageHandler m

transfer :: IO ()
transfer = do
  conn <- getConnection
  let partition = 0
      host = "localhost:9092"
      topic = "mediaTransfer"
      kafkaConfig = []
      topicConfig = []
  withKafkaConsumer kafkaConfig topicConfig
                    host topic
                    partition
                    KafkaOffsetStored
                    $ \kafka topic -> forever $ do
  consumed <- consumeMessage topic partition 1000
  let process = makeanio . decodeTransferMessage
  handleConsume consumed handleKafkaError (handleKafkaMessage process)

--watchTransfer :: IO ()

main :: IO ()
main = print "transfer"
