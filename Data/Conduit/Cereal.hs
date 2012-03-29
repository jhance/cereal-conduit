{-# LANGUAGE DeriveDataTypeable #-}
-- | Turn a 'Get' into a 'Sink' and a 'Put' into a 'Source'
module Data.Conduit.Cereal where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Typeable (Typeable)
import Control.Exception (Exception)
import qualified Data.Conduit as DC
import Data.Conduit.List (sourceList)
import Data.Serialize.Get
import Data.Serialize.Put

data GetException = GetException String
                  | GetDoesntConsumeInput
  deriving (Show, Typeable)

instance Exception GetException

-- | Convert a 'Get' into a 'Sink'. The 'Get' will be streamed bytes until it returns 'Done' or 'Fail'.
--
-- If the 'Get' fails, a GetException will be thrown with 'resourceThrow'. This function itself can also throw a GetException.
sinkGet :: Monad m => Get output -> DC.Sink BS.ByteString m (Maybe output)
sinkGet get = case runGetPartial get BS.empty of
                Fail _ -> DC.Done Nothing Nothing
                Partial f -> DC.Processing (push f) (close f)
                Done r rest -> DC.Done (if BS.null rest
                                            then Nothing
                                            else Just rest
                                       ) (Just r)
  where push f input
          | BS.null input = DC.Processing (push f) (close f)
          | otherwise = case f input of
              Fail _ -> DC.Done Nothing Nothing
              Partial f' -> DC.Processing (push f') (close f')
              Done r rest -> DC.Done (if BS.null rest
                                        then Nothing
                                        else Just rest
                                     ) (Just r)
        close _ = return Nothing

sourcePut :: Monad m => Put -> DC.Source m BS.ByteString
sourcePut put = sourceList $ LBS.toChunks $ runPutLazy put
