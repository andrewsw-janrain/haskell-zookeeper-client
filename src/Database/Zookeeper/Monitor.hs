module Monitor ( registerDataWatcher
               , registerChildrenWatcher
               , registerChildrenDataWatcher
               , ZooMonitor
               ) where

import Control.Applicative    ( (<$>) )
import Control.Exception
import Control.Monad
import Data.ByteString        (ByteString)
import Data.HashMap.Strict as HashMap
import Data.IORef
import Data.List              ( (\\) )
import Database.Zookeeper

type Path = String

type WatcherCallback a = a -> IO ()
type DataCallback = WatcherCallback (Maybe ByteString)
type ParentCallback = WatcherCallback ([Path], [Path])

data Watcher
     -- | a Zookeeper DataWatcher. Callback will be called with the
     -- most recent contents of the watched node upon event
     = DataWatcher DataCallback

     -- | a Zookeeper DataWatcher specifically for child nodes of a
     -- ChildrenDataWater. These watchers are not directly refreshed,
     -- but indirectly through the refresh of a ChildrenDataWatcher
     | ChildDataWatcher DataCallback

     -- | a Zookeeper ChildrenWatcher. Callback will receive a tuple
     -- containing the list of new children and list of deleted
     -- children of the watched node
     | ChildrenWatcher [Path] ParentCallback

     -- | a hybrid of DataWatcher and ChildrenWatcher. The first
     -- callback will receive a tuple containing the list of new
     -- children and list of deleted children of the watched node when
     -- a child node is created or deleted. The second callback will
     -- receive the path of a child node as if a DataWatcher had been
     -- applied to it. All child nodes will be watched automatically,
     -- so this kind of watcher will respond to any changes in the
     -- hierarchy.
     | ChildrenDataWatcher [Path] ParentCallback DataCallback

     -- | a Zookeeper CreateWatcher which will be called with the path of the watched node when the node is created.
     | CreateWatcher DataCallback

type Watchers = HashMap Path [Watcher]

data ZooMonitor = ZooMonitor { zHandle :: ZHandle
                             , watchers :: IORef Watchers
                             }

registerDataWatcher :: ZooMonitor -> Path -> DataCallback -> IO ()
registerDataWatcher = registerDataWatcher' DataWatcher

registerDataWatcher' :: (DataCallback -> Watcher) -> ZooMonitor -> Path -> DataCallback -> IO ()
registerDataWatcher' ctor zm path fn = do
  res <- try $ get (zHandle zm) path Watch
  case res of
    Right (val, _) -> do
      attempt <- try $ fn val
      case attempt of
        Right () -> do
          atomicModifyIORef (watchers zm) $ \ws ->
            let ws' = insertWith (++) path [ctor fn] ws
            in (ws' `seq` ws', ())
        Left e -> throw (e::SomeException)
    Left e@(ErrNoNode _) -> throw e -- node does not exist, how can we watch it? die...
    Left _ -> undefined -- certain classes of errors we want to
                        -- recover from, like ErrClosing,
                        -- ErrSessionExpired, ErrConnectionLoss, we
                        -- want to try again after the session comes
                        -- back up (if it does) so register ourselves
                        -- for that event

registerChildDataWatcher :: ZooMonitor -> Path -> DataCallback -> IO ()
registerChildDataWatcher = registerDataWatcher' ChildDataWatcher 


registerChildrenWatcher :: ZooMonitor -> Path -> ParentCallback -> IO ()
registerChildrenWatcher zm path fn = do
  res <- try $ getChildren (zHandle zm) path Watch
  case res of
    Right children -> do
      attempt <- try $ fn (children, [])
      case attempt of
        Right () -> atomicModifyIORef (watchers zm) $ \ws ->
            let ws' = insertWith (++) path [ChildrenWatcher children fn] ws
            in (ws' `seq` ws', ())
        Left e -> throw (e::SomeException)
    Left e@(ErrNoNode _) -> throw e
    Left _ -> undefined -- see registerDataWatcher ... we want the same thing here

registerChildrenDataWatcher :: ZooMonitor -> Path -> ParentCallback -> DataCallback -> IO ()
registerChildrenDataWatcher zm path pfn cfn = do
  res <- try $ getChildren (zHandle zm) path Watch
  case res of
    Right children -> do
      attempt <- try $ pfn (children, [])
      case attempt of
        Right () -> do
          atomicModifyIORef (watchers zm) $ \ws ->
            let ws' = insertWith (++) path [ChildrenDataWatcher children pfn cfn] ws
            in (ws' `seq` ws', ())
          forM_ children $ \child -> registerChildDataWatcher zm child cfn
        Left e -> throw (e::SomeException)
    Left e@(ErrNoNode _) -> throw e
    Left _ -> undefined -- same as above

watcher :: ZooMonitor -> ZHandle -> EventType -> State -> Path -> IO ()
watcher zm zh event Connected path =
  case event of
    Changed -> handleChanged zm zh path
    Child -> handleChildren zm zh path
    Deleted -> handleDeleted zm path
    Session -> undefined -- this means we just reconnected, run any pending actions
    _ -> undefined -- events we don't care about?
watcher _ _ _ ExpiredSession _ = undefined -- reconnect...
watcher _ _ _ Connecting _ = undefined -- wait for it...
watcher _ _ _ _ _ = undefined -- other states that we might care about?

------------------------------------------------------------------------
-- handle a Changed event for a data watch
--
-- if the get fails (meaning the node was deleted before we handled
-- the change, is this possible?), then delete the watcher from our
-- state
--
handleChanged :: ZooMonitor -> ZHandle -> Path -> IO ()
handleChanged zm zh path = do
    res <- try $ get zh path Watch
    case res of
      Right (val, _) ->  fns >>= flip forM_ (\fn -> fn val)
      Left (ErrNoNode _) -> atomicModifyIORef (watchers zm) $ \ws ->
        let ws' = HashMap.delete path ws
        in (ws' `seq` ws', ())
      Left _ -> undefined -- what do we do here for different error types? queue this action?
  where fns = dataWatchers . HashMap.lookupDefault [] path <$> readIORef (watchers zm)


------------------------------------------------------------------------
-- handle a Child event
--
-- For a ChildrenWatcher, just compute the changes and call the
-- function. For a ChildrenDataWatcher, compute the changes, call the
-- parent callback with the changes, then register a data watcher on
-- each new child
--

handleChildren :: ZooMonitor -> ZHandle -> Path -> IO ()
handleChildren zm zh path = do
    children <- getChildren zh path Watch
    ws >>= flip forM_ (applyWatcher children)
  where ws = HashMap.lookupDefault [] path <$> readIORef (watchers zm)
        applyWatcher cs w =
          let childChanges known = (cs \\ known, known \\ cs)
          in case w of
            ChildrenWatcher knownChildren fn -> fn $ childChanges knownChildren
            ChildrenDataWatcher knownChildren pfn cfn -> do
              let cs'@(newChildren, _) = childChanges knownChildren
              pfn cs'
              forM_ newChildren $ \child -> registerDataWatcher zm child cfn
            _ -> error "impossible handleChildren -- child watch event on improper Watcher"


------------------------------------------------------------------------
-- handle a Deleted event
--
handleDeleted :: ZooMonitor -> Path -> IO ()
handleDeleted zm path = atomicModifyIORef (watchers zm) $ \ws ->
  let ws' = HashMap.delete path ws
  in (ws' `seq` ws', ())

------------------------------------------------------------------------
------------------------------------------------------------------------
--
-- unexported utility functions
--


------------------------------------------------------------------------
-- extract only the data watchers from a list of watchers
dataWatchers :: [Watcher] -> [DataCallback]
dataWatchers [] = []
dataWatchers ((DataWatcher cb) : ws) = cb : dataWatchers ws
dataWatchers (_:ws) = dataWatchers ws
