package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: ExternalServiceProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val PAYMENT_PROVIDER_HOST_PORT: String = "localhost:1234"
    private val httpClientExecutor = Executors.newFixedThreadPool(256, NamedThreadFactory("http-client-executor"))
    private val javaClient = HttpClient.newBuilder()
        .executor(httpClientExecutor)
        .version(HttpClient.Version.HTTP_2)
        .build()

//    private val client = OkHttpClient.Builder()
//        .connectTimeout(requestAverageProcessingTime.multipliedBy(2))
//        .readTimeout(requestAverageProcessingTime.multipliedBy(7))
//        .writeTimeout(requestAverageProcessingTime.multipliedBy(2))
//        .retryOnConnectionFailure(true)
//        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
//        .dispatcher(Dispatcher().apply {
//            maxRequests = 1000
//            maxRequestsPerHost = 1000
//        })
//        .build()

    private val fixedRateLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1, TimeUnit.MILLISECONDS)
    private val nonBlockingOngoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request =  HttpRequest.newBuilder()
            .uri(URI("http://${PAYMENT_PROVIDER_HOST_PORT}/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId"))
            .POST(BodyPublishers.noBody())
            .build()
//        val request = Request.Builder().run {
//            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
//            post(emptyBody)
//        }.build()

        fixedRateLimiter.tickBlocking()
        nonBlockingOngoingWindow.putIntoWindow()
        var success = false
        while (!success && now() < deadline - requestAverageProcessingTime.toMillis()) {
            javaClient.sendAsync(request, BodyHandlers.ofString())
                .thenApply {
                    val body = try {
                        mapper.readValue(it.body(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${it.statusCode()}, reason: ${it.body()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
//                        paymentESService.update(paymentId) {
//                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
//                        }

                    success = body.result
                    nonBlockingOngoingWindow.releaseWindow()
                }
                .exceptionally { e ->
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error(
                                "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                                e
                            )
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                e
                            )

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }

                    nonBlockingOngoingWindow.releaseWindow()
                }

//            client.newCall(request).enqueue(
//                object : Callback {
//                    override fun onResponse(call: Call, response: Response) {
//                        val body = try {
//                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
//                        } catch (e: Exception) {
//                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
//                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
//                        }
//
//                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
//
//                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
//                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
////                        paymentESService.update(paymentId) {
////                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
////                        }
//
//                        success = body.result
//                        nonBlockingOngoingWindow.releaseWindow()
//                    }
//
//                    override fun onFailure(call: okhttp3.Call, e: java.io.IOException): kotlin.Unit {
//                        when (e) {
//                            is SocketTimeoutException -> {
//                                logger.error(
//                                    "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
//                                    e
//                                )
//                                paymentESService.update(paymentId) {
//                                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
//                                }
//                            }
//
//                            else -> {
//                                logger.error(
//                                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
//                                    e
//                                )
//
//                                paymentESService.update(paymentId) {
//                                    it.logProcessing(false, now(), transactionId, reason = e.message)
//                                }
//                            }
//                        }
//
//                        nonBlockingOngoingWindow.releaseWindow()
//                    }
//                }
//            )
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()






