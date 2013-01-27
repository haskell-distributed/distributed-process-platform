{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE TemplateHaskell            #-}

-- | Types used throughout the ManagedProcess framework
module Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
  ( -- * Exported data types
    InitResult(..)
  , Condition(..)
  , ProcessAction(..)
  , ProcessReply(..)
  , CallHandler
  , CastHandler
  , InitHandler
  , TerminateHandler
  , TimeoutHandler
  , UnhandledMessagePolicy(..)
  , ProcessDefinition(..)
  , Dispatcher(..)
  , DeferredDispatcher(..)
  , ExitSignalDispatcher(..)
  , MessageMatcher(..)
  , Message(..)
  , CallResponse(..)
  ) where

import Control.Distributed.Process -- hiding (Message)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Internal.Types
  ( Recipient(..)
  , TerminateReason(..)
  )
import Control.Distributed.Process.Platform.Time

import Data.Binary hiding (decode)
import Data.DeriveTH
import Data.Typeable (Typeable)
import Prelude hiding (init)

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

data Message a =
    CastMessage a
  | CallMessage a Recipient
  deriving (Typeable)
$(derive makeBinary ''Message)

data CallResponse a = CallResponse a
  deriving (Typeable)
$(derive makeBinary ''CallResponse)

-- | Return type for and 'InitHandler' expression.
data InitResult s =
    InitOk s Delay {-
        ^ denotes successful initialisation, initial state and timeout -}
  | forall r. (Serializable r)
    => InitFail r -- ^ denotes failed initialisation and the reason

-- | The action taken by a process after a handler has run and its updated state.
-- See 'continue'
--     'timeoutAfter'
--     'hibernate'
--     'stop'
--
data ProcessAction s =
    ProcessContinue  s                -- ^ continue with (possibly new) state
  | ProcessTimeout   TimeInterval s   -- ^ timeout if no messages are received
  | ProcessHibernate TimeInterval s   -- ^ hibernate for /delay/
  | ProcessStop      TerminateReason  -- ^ stop the process, giving @TerminateReason@

-- | Returned from handlers for the synchronous 'call' protocol, encapsulates
-- the reply data /and/ the action to take after sending the reply. A handler
-- can return @NoReply@ if they wish to ignore the call.
data ProcessReply s a =
    ProcessReply a (ProcessAction s)
  | NoReply (ProcessAction s)

type CallHandler a s = s -> a -> Process (ProcessReply s a)

type CastHandler s = s -> Process ()

-- type InfoHandler a = forall a b. (Serializable a, Serializable b) => a -> Process b

-- | Wraps a predicate that is used to determine whether or not a handler
-- is valid based on some combination of the current process state, the
-- type and/or value of the input message or both.
data Condition s m =
    Condition (s -> m -> Bool)  -- ^ predicated on the process state /and/ the message
  | State     (s -> Bool) -- ^ predicated on the process state only
  | Input     (m -> Bool) -- ^ predicated on the input message only

-- | An expression used to initialise a process with its state.
type InitHandler a s = a -> Process (InitResult s)

-- | An expression used to handle process termination.
type TerminateHandler s = s -> TerminateReason -> Process ()

-- | An expression used to handle process timeouts.
type TimeoutHandler s = s -> Delay -> Process (ProcessAction s)

-- dispatching to implementation callbacks

-- | Provides dispatch from cast and call messages to a typed handler.
data Dispatcher s =
    forall a . (Serializable a) => Dispatch {
        dispatch :: s -> Message a -> Process (ProcessAction s)
      }
  | forall a . (Serializable a) => DispatchIf {
        dispatch   :: s -> Message a -> Process (ProcessAction s)
      , dispatchIf :: s -> Message a -> Bool
      }

-- | Provides dispatch for any input, returns 'Nothing' for unhandled messages.
data DeferredDispatcher s = DeferredDispatcher {
    dispatchInfo :: s
                 -> AbstractMessage
                 -> Process (Maybe (ProcessAction s))
  }

-- | Provides dispatch for any exit signal - returns 'Nothing' for unhandled exceptions
data ExitSignalDispatcher s = ExitSignalDispatcher {
    dispatchExit :: s
                 -> ProcessId
                 -> AbstractMessage
                 -> Process (Maybe (ProcessAction s))
  }

class MessageMatcher d where
    matchMessage :: UnhandledMessagePolicy -> s -> d s -> Match (ProcessAction s)

instance MessageMatcher Dispatcher where
  matchMessage _ s (Dispatch   d)      = match (d s)
  matchMessage _ s (DispatchIf d cond) = matchIf (cond s) (d s)

-- | Policy for handling unexpected messages, i.e., messages which are not
-- sent using the 'call' or 'cast' APIs, and which are not handled by any of the
-- 'handleInfo' handlers.
data UnhandledMessagePolicy =
    Terminate  -- ^ stop immediately, giving @TerminateOther "UnhandledInput"@ as the reason
  | DeadLetter ProcessId -- ^ forward the message to the given recipient
  | Drop                 -- ^ dequeue and then drop/ignore the message

-- | Stores the functions that determine runtime behaviour in response to
-- incoming messages and a policy for responding to unhandled messages.
data ProcessDefinition s = ProcessDefinition {
    apiHandlers  :: [Dispatcher s]     -- ^ functions that handle call/cast messages
  , infoHandlers :: [DeferredDispatcher s] -- ^ functions that handle non call/cast messages
  , exitHandlers :: [ExitSignalDispatcher s] -- ^ functions that handle exit signals
  , timeoutHandler :: TimeoutHandler s   -- ^ a function that handles timeouts
  , terminateHandler :: TerminateHandler s -- ^ a function that is run just before the process exits
  , unhandledMessagePolicy :: UnhandledMessagePolicy -- ^ how to deal with unhandled messages
  }

