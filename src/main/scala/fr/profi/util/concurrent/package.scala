package fr.profi.util

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import rx.lang.scala.Observable

package object concurrent {
  
  object ObservableUtils {
    // TODO: rename to subscribe result to future
    def observableToFuture[T](observable: Observable[T]): Future[T] = {
      val promise = Promise[T]()
  
      observable.single.subscribe(
        onNext = { result: T => 
          promise.success(result)
        },
        onError = { error: Throwable =>
          promise.failure(error)
        }
      )
      
      promise.future
    }
  }
  
  implicit class FutureExtras[T](val f: Future[T]) extends AnyVal {
    def andFinally(fn: => Unit)(implicit ec: ExecutionContext): Future[T] = {
      f.map(result => { fn; result }).recoverWith {
        case error => { fn; f }
      }
    }
  }
}