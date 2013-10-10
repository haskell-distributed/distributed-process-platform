{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE FlexibleInstances          #-}

module Control.Distributed.Process.Platform.ManagedProcess.FSM where

import Control.Applicative (Applicative, (<$>))
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TChan
  ( readTChan
  , writeTChan
  )
import Control.Distributed.Process
  ( newChan
  , nsend
  , receiveWait
  , receiveTimeout
  , match
  , matchAny
  , matchChan
  , handleMessage
  , unwrapMessage
  , die
  , Process
  , ProcessId
  , Message
  )
import Control.Distributed.Process.Platform
  ( ExitReason(..)
  )
import Control.Distributed.Process.Platform.Internal.Queue.Mailbox
  ( Mailbox
  )
import qualified Control.Distributed.Process.Platform.Internal.Queue.Mailbox as Mailbox
import Control.Distributed.Process.Platform.ManagedProcess.Internal.GenProcess
  ( TimeoutSpec
  , startTimer
  , pollTimer
  )
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable (Serializable)
import Control.Monad (MonadPlus(..))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ask)
import qualified Control.Monad.State as ST
  ( MonadState
  , StateT
  , get
  , put
  , modify
  , lift
  , runStateT
  )
import Data.Binary
import Data.Typeable (Typeable)
import GHC.Generics

-- | The /real/ runtime context in which a state machine
-- (i.e., finite state) transducer executes.
newtype SMT s a =
  SMT
  {
    runSMT :: ST.StateT s Process a
  } deriving ( Functor
             , Monad
             , ST.MonadState s
             , MonadIO
             , Typeable
             , Applicative)

data CCX = InCall | InCast | None
  deriving (Typeable)

-- | The /real/ (i.e., internal) state of a state machine transducer.
data StateMachine s = SM { state   :: !s
                         , ccx     :: !CCX
                         , mailbox :: !Mailbox
                         , timer   :: TimeoutSpec
                         } deriving (Typeable)

-- | The definition of a finite state machine.
-- Our implementation corresponds to a fininte state automata,
-- maintaining state data of type @s@, where each state's
-- output value(s) can depend on the current state (data), one or
-- more inputs to the machine, or both. Slightly more formally, @FSM s@
-- is presented as a simple finite state transducer (a la mealy), whose
-- various steps produce a new 'Transition'.
type FSM s = SMT (StateMachine s) Transition

-- Describe how we should handle our mailbox during 'await' and co.

-- | A twist on @UnhandledMessagePolicy@, we can enqueue and/or timeout
-- messages during @await@ calls.
data UnhandledEventRetentionPolicy =
    Drop              -- drop unhandled messages on the floor
  | Enqueue           -- enqueue unhandled message in FIFO order
  | Terminate         -- stop the FSM if we encounter unhandled messages
  deriving (Typeable, Generic, Show)
instance Binary UnhandledEventRetentionPolicy where

data BlockSpec =
    NonBlocking
    {
      policy :: UnhandledEventRetentionPolicy
    }
  | Blocking
    {
      delay  :: TimeInterval
    , policy :: UnhandledEventRetentionPolicy
    }
  deriving (Typeable, Generic, Show)
instance Binary BlockSpec where

type Response = Maybe Message

-- | A transition between states.
data Transition =
    Await BlockSpec
  | Yield
  | Stop ExitReason
  deriving (Typeable, Generic, Show)
instance Binary Transition where

data SelectiveReceive m =
    Match Mailbox m
  | NoMatch Mailbox
  | Timeout Mailbox
  | Error

-- (:>) :: FSM s ->

-- | Sequentially compose two states, passing the state data yielded
-- by the first as an argument to the second. This creates a moore-like
-- machine, whose output values depend only on its current state. This
-- is useful if you wish to transition between states without the need
-- to wait for further input, however note that if the first state
-- becomes a waiting state (i.e., returns an @await@ transition),
-- then the mailbox will be drained prior to entering the second.
--
(|>) :: FSM s -> (s -> FSM s) -> FSM s
(|>) s1 s2 = do
  t <- s1
  case t of
    Await _ -> drainMailbox >> ST.get >>= s2 . state
    Yield   -> ST.get >>= s2 . state
    Stop _  -> return t

-- | The reverse of @(|>)@.
-- @(<|) = flip (|>)@
--
(<|) :: (s -> FSM s) -> FSM s -> FSM s
(<|) = flip (|>)

-- | Sequentially compose two states, passing
(~>) :: (Serializable m)
     => FSM s
     -> (m -> FSM s)
     -> FSM s
(~>) s1 s2 = do
  t <- s1
  case t of
    Await s -> awaitEvent s s2
    Yield   -> error "what does /this/ actually mean!?!?!?!?"
    _       -> return t -- can only be stop

-- | The reverse of @(~>)@.
-- @(<~) = flip (~>)@
--
(<~) :: (Serializable m)
     => (m -> FSM s)
     -> FSM s
     -> FSM s
(<~) = flip (~>)

handleAnyStateEvent :: (Serializable m) => (m -> FSM s) -> FSM s
handleAnyStateEvent st = do
  Just msg <- ST.get >>= return . Mailbox.peekFirst . mailbox
  handled <- handleMessage msg st
  case handled of
    Nothing -> error "what now!?"
    Just t  -> error "handle the transition!!!"

await :: FSM s
await = return $ Await $ NonBlocking Enqueue

demo :: SMT s Transition
demo = undefined

yield :: FSM s
yield = return Yield

-- | TODO MonadTrans instance? lift :: (Monad m) => m a -> t m a
lift :: Process a -> SMT s a
lift p = SMT $ ST.lift p

stop :: ExitReason -> FSM s
stop = return . Stop

halt :: ExitReason -> s -> FSM s
halt r _ = stop r

complete :: s -> FSM s
complete = halt ExitNormal

serve :: s -> FSM s -> Process ()
serve s fsm = do
  (t, _) <- initFSM s fsm
  case t of
    Stop r -> close r
    _      -> close $ ExitOther ("Unexpected Transition: " ++ (show t))
  where
    close ExitNormal = return ()
    close r          = die r

initFSM :: s -> FSM s -> Process (Transition, StateMachine s)
initFSM initState fsm = do
  t <- startTimer Infinity
  runFSM fsm $ SM { state   = initState
                  , ccx     = None
                  , mailbox = Mailbox.empty
                  , timer   = t
                  }

runFSM :: forall s.
          SMT (StateMachine s) Transition
       -> StateMachine s
       -> Process (Transition, StateMachine s)
runFSM fsm st = ST.runStateT (runSMT fsm) st

-- TODO: apply limits (count, timer, etc) to mailbox draining

drainMailbox :: SMT (StateMachine s) ()
drainMailbox = do
  SM{..} <- ST.get
  mb <- lift $ drainMailboxAux mailbox
  ST.modify $ \st -> st { mailbox = mb }
  where
    drainMailboxAux :: Mailbox -> Process Mailbox
    drainMailboxAux mb = do
      msg <- receiveTimeout 0 [ matchAny return ]
      case msg of
        Nothing -> return mb
        Just m  -> drainMailboxAux $ Mailbox.enqueue mb m

awaitEvent :: (Serializable m) => BlockSpec -> (m -> FSM s) -> FSM s
awaitEvent spec trans = do
  let pol = policy spec
  case pol of
    Drop -> do
      mb <- return . mailbox =<< ST.get
      result <- Mailbox.dropUntil mb $ \rm -> do
        handleMessage rm (\(m' :: m) -> return m')
      case result of
        Right (mb', Just msg) -> updateInbox mb' >> trans msg
        Right (mb', Nothing)  -> error "The impossible happened"
        Left  _               -> do clearMailbox -- fingerprint never matched
                                    drainAux spec >>= enforce spec trans
    Enqueue -> do
      mb <- return . mailbox =<< ST.get
      -- This is a bit unfortunate: we build up the returned mailbox (in the
      -- correct order) during select, then throw it away if we didn't find a
      -- match....
      (mb', handled) <- Mailbox.select mb trans
      case handled of
        Nothing -> drainAux spec >>= enforce spec trans
        Just ns -> updateInbox mb' >> return ns
    Terminate -> do
      mb <- return . mailbox =<< ST.get
      (mb', handled) <- Mailbox.select mb trans
      case handled of
        Nothing -> error "stop"
        Just ns -> updateInbox mb' >> return ns

  where

    drainAux :: (Serializable m)
             => BlockSpec
             -> SMT (StateMachine s) (SelectiveReceive m)
    drainAux spec = do
      let (recv, p) =
            case spec of
              NonBlocking p' -> (\ms -> receiveWait ms >>= return . Just, p')
              Blocking d  p' -> (receiveTimeout (asTimeout d), p')
      SM { mailbox = mb } <- ST.get
      lift $ recv [
                 match    (\(msg :: m) -> return (Match mb msg))
               , matchAny (\rm -> applyPolicy p mb rm)
               ] >>= maybe (return $ Timeout mb) return

    enforce :: (Serializable m)
            => BlockSpec
            -> (m -> FSM s)
            -> SelectiveReceive m
            -> FSM s
    enforce spec trans' sr
      | Match mb m' <- sr = updateInbox mb >> trans' m'
      | NoMatch mb  <- sr = updateInbox mb >> awaitEvent spec trans'
      | otherwise         = error "not sure about this one..."

    updateInbox :: Mailbox -> SMT (StateMachine s) ()
    updateInbox mb = ST.modify (\s -> s { mailbox = mb })

    clearMailbox = updateInbox Mailbox.empty

    applyPolicy :: (Serializable m)
                => UnhandledEventRetentionPolicy
                -> Mailbox
                -> Message
                -> Process (SelectiveReceive m)
    applyPolicy p mb rm
      | Drop      <- p = return $ NoMatch mb
      | Enqueue   <- p = return $ NoMatch (Mailbox.enqueue mb rm)
      | Terminate <- p = return $ Error

--  case Queue.dequeue mb of
--    Just (m, mb') -> return (sm { mailbox = mb' }, m)
--    Nothing       -> return (sm, Nothing)

{-

data ServerState = Idle (Map String Bool) | Connected | Invalid

run = fsm $ withState newDB $
idle = auth |> connected
auth = await :> checkPassword

connected Connected = await ~> echo |> connected
connected Invalid   = stop

checkPassword (Idle db) pwd =
  case lookup pwd db of
    True  -> yield Connected
    False -> yield Invalid

-----------------------------------

type Code = String
data State = Locked Code | Unlocked

initialState = unlocked
unlocked = yield Unlocked |> await ~> open <|> lock
unlocked = yield Unlocked |> await :> lock

locked = await ~> handleStateEvent [open, withState unlock]

lock Unlocked code = yield (Locked code) ~> reply >> locked
lock Locked   _    = refuse

open Locked   = refuse  -- jumps to the next state
open Unlocked = comply

unlock (Locked c) c' = if c == c' then reply unlocked else refuse
unlock _          _  = continue

from the above, we get the following combinators....

-- transitions based on only the current input

(~>) as |> but for inputs (only)
(<~) reverse of ~>
(<~>) returns to the LHS state if we cannot enter the RHS,
      which makes it act like a kind of short-circuit operator

-- transitions based on both the current state and input

(:>) as ~> and |>
(<:) as <~ and <|
(<:>) as <|> and <~>

-- transitions based on the current state i.e., whatever is 'yielded'

(<|>) creates an alternative (i.e., either, or) state, where we enter
      either the left or right state depending on the current state data
(|>) Sequentially compose two states, passing the state data yielded
     by the first as an argument to the second
(<|) reverse of |>

`yield` sets the state data

-}

{-

(|>) :: forall a s k o. (Serializable k, Step a)
     => FSM s (Transition s k)
     -> a
     -> FSM s (Transition s o)
(|>) t v = do
  t' <- t
  case t' of
    Await b    -> processMailbox b v
    Yield k    -> matchStep v
    s@(Stop _) -> error "fuck" -- return s :: FSM s (Transition o)
  where
--    processMailbox :: Int -> (k -> FSM s (Transition o)) -> FSM s (Transition o)
    processMailbox = undefined


data Step = Bind |

data Node s =
    Node s
  | Alt (Node s) (Node s)
  |

-- liftP = ST.lift

await :: FSM s (Transition s k)
await = return $ Await $ Blocking Infinity

require :: forall s k. (Serializable k) => FSM s k
require = undefined -- wait + drop intermediate results, notifying the caller if possible

-- expect = ensure
ensure :: forall s k. (Serializable k) => FSM s k
ensure = undefined -- wait + die on invalid input!

-- TODO: listen properly!

yield :: forall s k. (Serializable k)
      => k
      -> FSM s (Transition s k)
yield = return . Yield

stop :: forall s k. FSM s (Transition s k)
stop = return $ (Stop ExitNormal :: Transition s k)

exit :: forall s k. ExitReason -> FSM s (Transition s k)
exit r = return $ (Stop r :: Transition s k)

-- what I think we want, is a way to express Dispatchers in a less verbose fashion...

-- in connected...
-- await tells us how to wait (i.e., selectively block + cache intermediates)
-- echo says what to do with a message (and what its type must be)
-- even if echo fails, *> says we go to connected

-- in idle...
-- require tells us how to wait (i.e., wait + drop)
-- if auth fails, it recurses (the Left case)
-- if auth succeeds, we go to connected (the Right case)

class Step a where
  matchStep :: a -> k -> s -> FSM s (Transition s o)

A step, such as await and yield, must be executable
with (or without) input!

start = idle

-- store = putState
-- await = step

await fsm =
  m <- receive match-for fsm
  case m of
    Just match -> return $ Step $ fsm match
    Nothing    -> return $ Failed m

(<|>) fsmLeft fsmRight = return $ OneOf fsmLeft fsmRight

run = fsm $ withState newDB $ idle

idle = require |> auth <| connected

connected = await |> echo *> connected

idle = withState newDB >> auth >>= connected <|> idle

auth = do
  Login{..} <- require  -- could've been await or expect/ensure
  db <- getState
  case (isAuthorised username password db) of
    Left (user, token) -> do
      modifyState $ (^: Map.insert user token)
      yield connected
    Right err ->
      reply NotAllowed
      stop

connected = await (echo input) *> connected

echo filter = handleEvent filter

input :: String -> Bool
input _ = True

-}

