module Monitor where

import Control.Exception
import Control.Monad
import Data.ByteString (ByteString)
import Data.HashMap.Strict
import Data.IORef
import Database.Zookeeper

type Path = String

type WatcherCallback a = a -> IO ()

data Watcher
     -- | a Zookeeper DataWatcher. Callback will be called with the path of the watched node upon event
     = DataWatcher (WatcherCallback (Maybe ByteString))

     -- | a Zookeeper ChildrenWatcher. Callback will receive the current list of children of the watched node
     | ChildrenWatcher (WatcherCallback [Path])

     -- | a hybrid of DataWatcher and ChildrenWatcher. The first
     -- callback will receive the current list of children of the
     -- watched node when a child node is created or deleted. The
     -- second callback will receive the path of a child node as if a
     -- DataWatcher had been applied to it. All child nodes will be
     -- watched automatically, so this kind of watcher will respond to
     -- any changes in the hierarchy.
     | ChildDataWatcher (WatcherCallback [Path]) (WatcherCallback (Maybe ByteString))

     -- | a Zookeeper CreateWatcher which will be called with the path of the watched node when the node is created.
     | CreateWatcher (WatcherCallback Path)

type Watchers = HashMap Path [Watcher]

data ZooMonitor = ZooMonitor { zHandle :: ZHandle
                             , watchers :: IORef Watchers
                             }

registerDataWatcher :: ZooMonitor -> Path -> (WatcherCallback (Maybe ByteString)) -> IO ()
registerDataWatcher zm path fn = do
  res <- try $ get (zHandle zm) path Watch
  case res of
    Right (val, stat) -> do
      attempt <- try $ fn val
      case attempt of
        Right () -> do
          atomicModifyIORef (watchers zm) $ \ws ->
            let ws' = insertWith (++) path [DataWatcher fn] ws
            in (ws' `seq` ws', ())
        Left e -> throw (e::SomeException)
    Left e@(ErrNoNode _) -> throw e -- node does not exist, how can we watch it? die...
    Left _ -> undefined -- certain classes of errors we want to
                        -- recover from, like ErrClosing,
                        -- ErrSessionExpired, ErrConnectionLoss, we
                        -- want to try again after the session comes
                        -- back up (if it does) so register ourselves
                        -- for that event

registerChildrenWatcher :: ZooMonitor -> Path -> (WatcherCallback [Path]) -> IO ()
registerChildrenWatcher zm path fn = do
  res <- try $ getChildren (zHandle zm) path Watch
  case res of
    Right children -> do
      attempt <- try $ fn children
      case attempt of
        Right () -> do
          atomicModifyIORef (watchers zm) $ \ws ->
            let ws' = insertWith (++) path [ChildrenWatcher fn] ws
            in (ws' `seq` ws', ())
        Left e -> throw (e::SomeException)
    Left e@(ErrNoNode _) -> throw e
    Left _ -> undefined -- see registerDataWatcher ... we want the same thing here

watcher :: ZHandle -> EventType -> State -> Path -> IO ()
watcher zh event Connected path =
  case event of
    Changed -> handleChanged zh path
    Child -> handleChildren zh path
    Deleted -> undefined -- remove the watcher from the map, but... 
    Session -> undefined -- this means we just reconnected, run any pending actions
    _ -> undefined -- events we don't care about?
watcher _ _ ExpiredSession _ = undefined -- reconnect... 
watcher _ _ Connecting _ = undefined -- wait for it...
watcher _ _ _ _ = undefined -- other states that we might care about?

handleChanged :: ZHandle -> Path -> IO ()
handleChanged zh path = do
    (val, _) <- get zh path Watch
    forM_ fns (\fn -> fn val)
  where fns = undefined :: [WatcherCallback (Maybe ByteString)]


handleChildren :: ZHandle -> Path -> IO ()
handleChildren zh path = do
    children <- getChildren zh path Watch
    forM_ ws $ \w ->
      case w of
        ChildrenWatcher fn -> fn children
        ChildDataWatcher pfn cfn -> do
          pfn children
          forM_ children $ \child -> do
            (val, _) <- get zh child Watch
            cfn val
        _ -> error "impossible handleChildren -- child watch event on improper Watcher"
    return ()
  where ws = undefined :: [Watcher]