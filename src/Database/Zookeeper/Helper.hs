{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DoRec #-}
module Database.Zookeeper.Helper where

import           Control.Applicative   hiding (empty, many)
import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Data.ByteString       (ByteString)
import           Data.HashMap.Strict (HashMap, empty)
import qualified Data.HashMap.Strict as HashMap
import           Data.IORef
import           Data.List (isPrefixOf)
import           Data.Maybe
import           GHC.Conc  (ThreadStatus, threadStatus)
import           System.Log.Logger hiding (Logger)
import qualified Database.Zookeeper as Z
import           Database.Zookeeper hiding (init)

import           Prelude hiding (catch, init)

--------------------------------------------------------------------------------
data ZooHelper = ZooHelper
    { zooRef  :: IORef ZooHelper'
    , connStr :: String
    , timeout :: Int
    , logger  :: Logger
    }

data EphemRecord = EphemRecord
    { ePath     :: String
    , eValue    :: Maybe ByteString
    , eSeq      :: Bool
    }

type Logger = Priority -> String -> IO ()

type GetCb = Bool -> String -> Maybe ByteString -> Stat -> IO ()

type ParentCb = Bool -> String -> [String] -> IO ()

data ZooHelper' = ZooHelper'
    { zH            :: ZHandle
    , refreshThread :: ThreadId
    , getCbs        :: HashMap String [GetCb]
    , parentCbs     :: HashMap String [ParentCb]
    , childCbs      :: HashMap String GetCb
    , ephemerals    :: [EphemRecord]
    }

--------------------------------------------------------------------------------
init :: String -> Int -> IO ZooHelper
init connStr timeout = do
    zh <- Z.init connStr Nothing timeout
    let logger = logM "zookeeper"
    rec let z = ZooHelper { .. }
        zooRef <- newIORef ZooHelper' { zH         = zh
                                      , getCbs     = empty
                                      , parentCbs  = empty
                                      , childCbs   = empty
                                      , ephemerals = []
                                      , refreshThread = refreshThread'
                                      }
        refreshThread' <- forkIO $ refreshZookeeper z

    Z.setWatcher zh $ Just $ watcher z
    return z


zHandle :: ZooHelper -> IO ZHandle
zHandle ZooHelper {..} = zH <$> readIORef zooRef


-- should we define an error type to enumerate the possible errors, and take a
-- callback to handle all errors?
watcher :: ZooHelper -> ZHandle -> EventType -> State -> String -> IO ()
watcher z@ZooHelper{..} zh eventType st path =
    case (eventType, st) of
        (Session, ExpiredSession) -> handleErr handleExpired
        (Session, Connected)      -> logger NOTICE "zookeeper session connected"
        (Session, Connecting)     -> logger NOTICE "zookeeper is connecting"
        (Changed, Connected)      -> handleErr handleChanged
        (Deleted, Connected)      -> return () -- nothing to do here, right?
        (Child, Connected)        -> handleErr handleChild
        _ -> logger CRITICAL $ concat [ "Unrecognized event: ", reportStr ]
  where
    debugOut = logger DEBUG $ concat [ "Got event: ", reportStr ]

    reportStr = concat [ show eventType, " ", show st, " ", path ]

    handleErr action =
        action `catch` \e ->
        logger CRITICAL $ concat [ "Exception when handling event: "
                                 , reportStr, ", ", show (e :: SomeException)
                                 ]

    handleExpired = do

        logger CRITICAL "zookeeper session expired"

        ZooHelper' {..} <- readIORef zooRef

        -- kill the refresh thread, who knows what state it's in
        killThread refreshThread

        Z.close zH
        newZh <- Z.init connStr (Just $ watcher z) timeout
        modifyIORef zooRef (\x -> x { zH = newZh })

        -- filter out all old record of ephemeral data watchers -- they're gone now!
        let ePaths = map ePath ephemerals
            getCbs' = HashMap.filterWithKey (\cb _ -> and $ map (not . flip isPrefixOf cb) ePaths) getCbs
        modifyIORef zooRef (\x -> x { getCbs = getCbs' })

        reApplyWatchers z handleErr

        -- recreate ephemerals
        forM_ ephemerals (createEphemeral newZh logger)

        -- restart the refresh thread
        rt <- forkIO $ refreshZookeeper z
        modifyIORef zooRef (\x -> x { refreshThread = rt })

    handleChanged = do
        debugOut
        ZooHelper' {..} <- readIORef zooRef
        let fns = fromMaybe [] $ HashMap.lookup path getCbs
            allFns = maybe fns (: fns) $ HashMap.lookup path childCbs
        runGetCbs zh logger False path $ allFns

    handleChild = do
        debugOut
        ZooHelper' {..} <- readIORef zooRef
        let fns = fromMaybe [] $ HashMap.lookup path parentCbs
        runParentCbs zh logger False path fns

--------------------------------------------------------------------------------
-- start watching a node and apply the given callback when the node changes. the
-- callback is also applied in this function on the initial value. if the node
-- does not exist, an exception is thrown and the callback is not registered.
-- any exception that occurs when applying the callback is caught and logged.
--
-- we don't know how often watch callbacks might fire, relative to how
-- often they are refreshed, thus we force the callback maps in a
-- couple of places to prevent leaks
watchGet :: ZooHelper -> String -> GetCb -> IO ()
watchGet z path fn = do
    zh <- zHandle z
    runGetCbs zh (logger z) True path [fn]
    atomicModifyIORef (zooRef z) $ \z' ->
      let updatedGetCbs = HashMap.insertWith (++) path [fn] $ getCbs z'
      in updatedGetCbs `seq` (z' { getCbs = updatedGetCbs }, ()) -- seq because we don't want to leak here


watchChildren :: ZooHelper -> String -> ParentCb -> IO ()
watchChildren z parent fn = do
    zh <- zHandle z
    runParentCbs zh (logger z) True parent [fn]
    atomicModifyIORef (zooRef z) $ \z' ->
      let updatedParentCbs = HashMap.insertWith (++) parent [fn] $ parentCbs z'
      in updatedParentCbs `seq` (z' { parentCbs = updatedParentCbs }, ()) -- seq because we don't want to leak here

watchChild :: ZooHelper -> String -> GetCb -> IO ()
watchChild z path fn = do
  zh <- zHandle z
  runGetCbs zh (logger z) True path [fn]
  atomicModifyIORef (zooRef z) $ \z' ->
    let updatedChildCbs = HashMap.insert path fn $ childCbs z'
    in updatedChildCbs `seq` (z' { childCbs = updatedChildCbs }, ()) -- seq because we don't want to leak here

registerEphemeral :: ZooHelper -> String -> Maybe ByteString -> Bool -> IO ()
registerEphemeral z path value sequential = do
    let ephem = EphemRecord { ePath  = path
                            , eValue = value
                            , eSeq   = sequential
                            }

    zh <- atomicModifyIORef (zooRef z) $ \z' ->
          (z' { ephemerals = ephem : ephemerals z' }, zH z')

    createEphemeral zh (logger z) ephem

--------------------------------------------------------------------------------
-- worker thread to periodically refresh all watches

refreshZookeeper :: ZooHelper -> IO ()
refreshZookeeper z = do
    threadDelay delay
    logZ NOTICE $ "refreshing all zookeeper watchers"
    reApplyWatchers z handleErr
    refreshZookeeper z
  where
    delay = 5 * 60 * 10^(6::Int) -- 5 minutes
    logZ = logger z
    handleErr action =
      action `catch` \e -> case fromException e of
        Just ThreadKilled -> throw e
        _ -> logZ CRITICAL $ concat [ "Exception when refreshing zookeeper watchers: "
                                    , show (e :: SomeException)
                                    ]

--------------------------------------------------------------------------------
-- check the status of the refresh worker thread

refreshStatus :: ZooHelper -> IO ThreadStatus
refreshStatus z = readIORef (zooRef z) >>= threadStatus . refreshThread

--------------------------------------------------------------------------------
-- reapply all recorded watches

reApplyWatchers :: ZooHelper -> (IO () -> IO ()) -> IO ()
reApplyWatchers ZooHelper{..} errHandler = do

    ZooHelper'{..} <- readIORef zooRef

    let runCbs runner refresh = uncurry $ runner zH logger refresh

    -- reattach watchers, note we catch errors to ensure we watch everything we can
    forM_ (HashMap.toList getCbs) (errHandler . runCbs runGetCbs False)
    forM_ (HashMap.toList parentCbs) (errHandler . runCbs runParentCbs True)


--------------------------------------------------------------------------------
-- non-exported utility functions

runGetCbs :: ZHandle -> Logger -> Bool -> String -> [GetCb] -> IO ()
runGetCbs zh logFn initial path fns = do
    (value, stat) <- get zh path Watch
    forM_ fns $ \fn ->
        fn initial path value stat `catch` \e ->
            logFn CRITICAL $
            concat [ "Exception when processing get callback for "
                   , path, ": ", show (e :: SomeException) ]

runParentCbs :: ZHandle -> Logger -> Bool -> String -> [ParentCb] -> IO ()
runParentCbs zh logFn initial parent fns = do
    children <- getChildren zh parent Watch
    forM_ fns $ \fn ->
        fn initial parent children `catch` \e ->
            logFn CRITICAL $
            concat [ "Exception when processing parent callback for "
                   , parent, ": ", show (e :: SomeException) ]


-- TODO catch exceptions
createEphemeral :: ZHandle -> Logger -> EphemRecord -> IO ()
createEphemeral zh logFn ephem = do
    let mode = CreateMode { create_ephemeral = True
                          , create_sequence  = eSeq ephem
                          }
    logFn NOTICE $ "creating ephemeral " ++ ePath ephem
    _ <- create zh (ePath ephem) (eValue ephem) OpenAclUnsafe mode
    return ()

--------------------------------------------------------------------------------
