module Database.Zookeeper.Util ( getJSON
                               , modify
                               , maybeCreate
                               , maybeDelete
                               , maybeGetJSON
                               , write
                               ) where

import           Control.Applicative
import           Control.Exception
import           Control.Monad
import           Control.Monad.Trans   (MonadIO(..))
import           Data.Aeson            as Aeson
import qualified Data.Aeson.Parser     as Aeson
import           Data.Attoparsec       (eitherResult, parse)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy  as BL
import           Data.Function         (fix)
import           Data.Maybe
import           Database.Zookeeper    as Zookeeper

-- misc. zookeeper utility functions.

--------------------------------------------------------------------------------
-- create a node if it does not exist, and populate it with a JSON value.
maybeCreate :: ZHandle -> String -> Maybe Aeson.Value -> IO ()
maybeCreate zh path val = do
    let val' = encodeJSON <$> val
    r <- try $ create zh path val' OpenAclUnsafe mode
    case r of
        Left (ErrNodeExists _) -> return ()
        Left err -> throw err
        Right _ -> return ()
  where
    mode :: CreateMode
    mode = CreateMode { create_ephemeral = False
                      , create_sequence  = False
                      }

--------------------------------------------------------------------------------
readJSON :: (Monad m, FromJSON a) => ByteString -> m a
readJSON str = case decodeJSON str of
    Left msg -> err $ "failed to parse: " ++ msg
    Right val -> case fromJSON val of
        Error msg -> err $ "failed to convert from JSON: " ++ msg
        Success a -> return a
  where
    err msg = error $ concat [ "readJSON: ", msg ]

--------------------------------------------------------------------------------
-- update a json value in zookeeper by applying a function to it.
modify :: (MonadIO m, FromJSON a, ToJSON a) =>
          ZHandle -> String -> (a -> m a) -> m ()
modify zh path f = fix $ \loop -> do
    (Just str, st) <- liftIO $ get zh path NoWatch
    a <- readJSON str
    a' <- f a
    let version = fromIntegral $ stat_version st
        str'    = encodeJSON $ toJSON a'
    b <- liftIO $ try_set zh path (Just str') version
    unless b loop

--------------------------------------------------------------------------------
try_set :: ZHandle -> String -> Maybe ByteString -> Int -> IO Bool
try_set zh path val version = do
    r <- try $ Zookeeper.set zh path val version
    case r of
        Left (ErrBadVersion _) -> return False
        Left err -> throw err
        Right () -> return True

--------------------------------------------------------------------------------
maybeDelete :: ZHandle -> String -> IO Bool
maybeDelete zh path = loop
  where
    loop = do
        mStat <- liftIO $ exists zh path NoWatch
        case mStat of
            Just stat -> do
                let version = fromIntegral $ stat_version stat
                b <- liftIO $ try_delete zh path version
                if b
                  then return True
                  else loop
            Nothing -> return False

--------------------------------------------------------------------------------
try_delete :: ZHandle -> String -> Int -> IO Bool
try_delete zh path version = do
    r <- try $ delete zh path version
    case r of
        Left (ErrBadVersion _) -> return False
        Left err -> throw err
        Right () -> return True

--------------------------------------------------------------------------------
getJSON :: FromJSON a => ZHandle -> String -> IO a
getJSON zh path = do
    (mStr, _) <- get zh path NoWatch
    readJSON $! fromMaybe (error $ "getJSON: no value in " ++ path) mStr

--------------------------------------------------------------------------------
maybeGetJSON :: FromJSON a => ZHandle -> String -> IO (Maybe a)
maybeGetJSON zh path = maybe (return Nothing) (const $ getJSON zh path) =<<
                       exists zh path NoWatch

--------------------------------------------------------------------------------
-- write a json value to a zookeeper node, ignoring the current value.
-- TODO define and use a Zookeeper function that ignores the version?
write :: (MonadIO m, FromJSON a, ToJSON a) =>
          ZHandle -> String -> a -> m ()
write zh path a = fix $ \loop -> do
    (_, st) <- liftIO $ get zh path NoWatch
    let version = fromIntegral $ stat_version st
        str     = encodeJSON $ toJSON a
    b <- liftIO $ try_set zh path (Just str) version
    unless b loop

--------------------------------------------------------------------------------
encodeJSON :: Aeson.Value -> ByteString
encodeJSON = B.concat . BL.toChunks . Aeson.encode

decodeJSON :: ByteString -> Either String Aeson.Value
decodeJSON = eitherResult . parse Aeson.value'

--------------------------------------------------------------------------------
