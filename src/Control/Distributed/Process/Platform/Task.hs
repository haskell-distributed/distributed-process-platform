-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Task
-- Copyright   :  (c) Tim Watson 2013 - 2014
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- The /Task Framework/ intends to provide tools for task management, work
-- scheduling and distributed task coordination. These capabilities build on the
-- /Execution Framework/ as well as other tools and libraries. The framework is
-- currently a work in progress. The current release includes a simple bounded
-- blocking queue implementation only, as an example of the kind of capability
-- and API that we intend to produce.
--
-- The /Task Framework/ will be broken down by the task scheduling and management
-- algorithms it provides, e.g., at a low level providing work queues, worker pools
-- and the like, whilst at a high level allowing the user to choose between work
-- stealing, sharing, distributed coordination, user defined sensor based bounds/limits
-- and so on.
--
-----------------------------------------------------------------------------
module Control.Distributed.Process.Platform.Task
  ( -- * Task Queues
    module Control.Distributed.Process.Platform.Task.Queue.BlockingQueue
  ) where

import Control.Distributed.Process.Platform.Task.Queue.BlockingQueue
