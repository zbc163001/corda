package net.corda.node.utilities.registration

import com.google.common.jimfs.Configuration.unix
import com.google.common.jimfs.Jimfs
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.SecureHash
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.cert
import net.corda.core.internal.createDirectories
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.crypto.loadKeyStore
import net.corda.testing.ALICE
import net.corda.testing.rigorousMock
import net.corda.testing.testNodeConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Test
import java.security.cert.X509Certificate
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class NetworkRegistrationHelperTest {
    private val fs = Jimfs.newFileSystem(unix())

    @After
    fun cleanUp() {
        fs.close()
    }

    @Test
    fun buildKeyStore() {
        val baseDirectory = fs.getPath("/baseDir").createDirectories()
        val requestId = SecureHash.randomSHA256().toString()

        val legalName = ALICE.name
        val intermediateCaName = CordaX500Name("CORDA_INTERMEDIATE_CA", "R3 Ltd", "London", "GB")
        val rootCaName = CordaX500Name("CORDA_ROOT_CA", "R3 Ltd", "London", "GB")

        val (nodeCaCert, intermediateCaCert, rootCaCert) = listOf(legalName, intermediateCaName, rootCaName).map {
            X509Utilities.createSelfSignedCACertificate(it, Crypto.generateKeyPair(X509Utilities.DEFAULT_TLS_SIGNATURE_SCHEME)).cert
        }

        val certService = rigorousMock<NetworkRegistrationService>().also {
            doReturn(requestId).whenever(it).submitRequest(any())
            doReturn(arrayOf(nodeCaCert, intermediateCaCert, rootCaCert)).whenever(it).retrieveCertificates(eq(requestId))
        }

        val config = testNodeConfiguration(baseDirectory = baseDirectory, myLegalName = legalName)

        assertThat(config.nodeKeystore).doesNotExist()
        assertThat(config.sslKeystore).doesNotExist()
        assertThat(config.trustStoreFile).doesNotExist()

        NetworkRegistrationHelper(config, certService).buildKeystore()

        assertThat(config.nodeKeystore).exists()
        assertThat(config.sslKeystore).exists()
        assertThat(config.trustStoreFile).exists()

        val nodeKeystore = loadKeyStore(config.nodeKeystore, config.keyStorePassword)
        val sslKeystore = loadKeyStore(config.sslKeystore, config.keyStorePassword)
        val trustStore = loadKeyStore(config.trustStoreFile, config.trustStorePassword)

        nodeKeystore.run {
            assertTrue(containsAlias(X509Utilities.CORDA_CLIENT_CA))
            assertFalse(containsAlias(X509Utilities.CORDA_INTERMEDIATE_CA))
            assertFalse(containsAlias(X509Utilities.CORDA_ROOT_CA))
            assertFalse(containsAlias(X509Utilities.CORDA_CLIENT_TLS))
            val nodeCaCertChain = getCertificateChain(X509Utilities.CORDA_CLIENT_CA)
            assertThat(nodeCaCertChain).containsExactly(nodeCaCert, intermediateCaCert, rootCaCert)
        }

        sslKeystore.run {
            assertFalse(containsAlias(X509Utilities.CORDA_CLIENT_CA))
            assertFalse(containsAlias(X509Utilities.CORDA_INTERMEDIATE_CA))
            assertFalse(containsAlias(X509Utilities.CORDA_ROOT_CA))
            assertTrue(containsAlias(X509Utilities.CORDA_CLIENT_TLS))
            val nodeTlsCertChain = getCertificateChain(X509Utilities.CORDA_CLIENT_TLS)
            assertThat(nodeTlsCertChain).hasSize(4)
            // The TLS cert has the same subject as the node CA cert
            assertThat(CordaX500Name.build((nodeTlsCertChain[0] as X509Certificate).subjectX500Principal)).isEqualTo(legalName)
            assertThat(nodeTlsCertChain.drop(1)).containsExactly(nodeCaCert, intermediateCaCert, rootCaCert)
        }

        trustStore.run {
            assertFalse(containsAlias(X509Utilities.CORDA_CLIENT_CA))
            assertFalse(containsAlias(X509Utilities.CORDA_INTERMEDIATE_CA))
            assertTrue(containsAlias(X509Utilities.CORDA_ROOT_CA))
            val trustStoreRootCaCert = getCertificate(X509Utilities.CORDA_ROOT_CA)
            assertThat(trustStoreRootCaCert).isEqualTo(rootCaCert)
        }
    }
}
