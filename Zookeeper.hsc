
{-# LANGUAGE ForeignFunctionInterface #-}

module Zookeeper (init, close, recvTimeout, WatcherFunc, State(..)) where

import Prelude hiding (init)

import Foreign
import Foreign.C.Types
import Foreign.C.Error
import Foreign.C.String

-- Exported data types:

data ZHBlob   = ZHBlob
data VoidBlob = VoidBlob -- C pointer placeholder

type ZHandle = ForeignPtr ZHBlob
type VoidPtr = Ptr VoidBlob

data State = ExpiredSession | AuthFailed | Connecting |
             Associating | Connected deriving(Show)

type WatcherImpl = Ptr ZHBlob -> Int -> Int -> CString -> VoidPtr -> IO ()
type WatcherFunc = ZHandle -> Int -> State -> String -> IO ()

-- Exported interface:

init  :: String -> WatcherFunc -> Int -> IO ZHandle
close :: ZHandle -> IO ()

recvTimeout :: ZHandle -> IO Int

-- C functions:

#include <zookeeper.h>

foreign import ccall "wrapper"
  wrapWatcherImpl :: WatcherImpl -> IO (FunPtr WatcherImpl)

foreign import ccall unsafe
  "zookeeper.h zookeeper_init" zookeeper_init ::
  CString -> FunPtr WatcherImpl -> Int ->
  VoidPtr -> VoidPtr -> Int -> IO (Ptr ZHBlob)

foreign import ccall unsafe
  "zookeeper_init.h &zookeeper_close" zookeeper_close_ptr ::
  FunPtr (Ptr ZHBlob -> IO ()) -- FIXME: IO Int

foreign import ccall unsafe
  "zookeeper.h zookeeper_close" zookeeper_close ::
  Ptr ZHBlob -> IO Int

foreign import ccall unsafe
  "zookeeper.h zoo_recv_timeout" zoo_recv_timeout ::
  Ptr ZHBlob -> IO Int

-- Internal functions:

wrapWatcher func =
  wrapWatcherImpl (\zhBlob zType zState csPath _ -> do
    path <- peekCString csPath
    zh <- newForeignPtr_ zhBlob
    func zh zType (zooState zState) path)

zooState (#const ZOO_EXPIRED_SESSION_STATE) = ExpiredSession
zooState (#const ZOO_AUTH_FAILED_STATE    ) = AuthFailed
zooState (#const ZOO_CONNECTING_STATE     ) = Connecting
zooState (#const ZOO_ASSOCIATING_STATE    ) = Associating
zooState (#const ZOO_CONNECTED_STATE      ) = Connected

-- Implementation of exported functions:

init host watcher timeout =
  withCString host (\csHost -> do
    watcherPtr <- wrapWatcher watcher
    zh <- throwErrnoIfNull
      ("zInit: " ++ host)
      (zookeeper_init csHost watcherPtr timeout nullPtr nullPtr 0)
    newForeignPtr zookeeper_close_ptr zh)

close = finalizeForeignPtr

recvTimeout zh = withForeignPtr zh zoo_recv_timeout

