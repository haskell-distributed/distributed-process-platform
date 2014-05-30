-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Service
-- Copyright   :  (c) Tim Watson 2013 - 2014
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- The /Service Framework/ is intended to provide a /service or component oriented/
-- API for developing cloud haskell applications. Ultimately, we aim to provide
-- a declarative mechanism for defining service components and their dependent
-- services/sub-systems so we can automatically derive an appropriate supervision
-- tree. This work is incomplete.
--
-- Access to services, both internally and from remote peers, should take place
-- via the /Registry/ module, with several different kinds of registry defined
-- per node plus user defined registries running where applicable. Again, this
-- is a work in progress, though the service registry capability is available
-- in the current release.
--
-- The service API also aims to provide some built in capabilities for common
-- tasks such as monitoring, management and logging. An extension of the base
-- Management (Mx) API that covers /ManagedProcess/ and /Supervision/ trees will
-- be also be added here in a future release.
--
-----------------------------------------------------------------------------
module Control.Distributed.Process.Platform.Service
  ( -- * Monitoring Nodes
    module Control.Distributed.Process.Platform.Service.Monitoring
    -- * Service Registry
  , module Control.Distributed.Process.Platform.Service.Registry
  ) where

import Control.Distributed.Process.Platform.Service.Monitoring
import Control.Distributed.Process.Platform.Service.Registry
