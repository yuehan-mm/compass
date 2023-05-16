package com.oppo.cloud.meta.service.impl;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.*;

public class HttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private boolean isSecurity = false;

    private String principal;

    private String keyTabLocation;

    private String krb5Location;

    CloseableHttpClient client;

    Subject serviceSubject;

    Thread renewer;

    private static Map<String, HttpClient> httpClientMap = new HashMap<>();

    public static synchronized HttpClient getInstance(String ip, boolean isSecurity, String principal, String keyTabLocation, String krb5Location) {
        HttpClient httpClient = httpClientMap.get("ip");
        if (httpClient == null) {
            httpClient = new HttpClient(isSecurity, principal, keyTabLocation, krb5Location);
            httpClientMap.put(ip, httpClient);
        }
        return httpClient;
    }

    private HttpClient() {
        this(false, null, null, null);
    }

    private HttpClient(boolean isSecurity, String principal, String keyTabLocation, String krb5Location) {
        this.isSecurity = isSecurity;
        if (isSecurity) {
            this.principal = principal;
            this.keyTabLocation = keyTabLocation;
            this.krb5Location = krb5Location;
        }
        init();
    }

    public void init() {
        List<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
        HttpClientBuilder builder = null;
        try {
            if (isSecurity) {
                if (null == krb5Location || krb5Location.trim().length() == 0)
                    throw new Exception("param error,krb5Location missing...");
                if (null == principal || principal.trim().length() == 0)
                    throw new Exception("param error,krb5Location missing...");
                if (null == keyTabLocation || keyTabLocation.trim().length() == 0)
                    throw new Exception("param error,keyTabLocation missing...");
            }
            builder = HttpClientBuilder.create();
            builder.setDefaultHeaders(headers);
            if (isSecurity) {
                System.setProperty("java.security.krb5.conf", krb5Location);
                client = buildSpengoHttpClient(builder);
                serviceSubject = getSubject();
                renewer = this.new KdcRenewer();
                renewer.start();
            } else {
                client = builder.build();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    private CloseableHttpClient buildSpengoHttpClient(HttpClientBuilder builder) {
        Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create().
                register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();
        builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(null, -1, null), new Credentials() {
            @Override
            public Principal getUserPrincipal() {
                return null;
            }

            @Override
            public String getPassword() {
                return null;
            }
        });
        builder.setDefaultCredentialsProvider(credentialsProvider);
        CloseableHttpClient client = builder.build();
        return client;
    }


    public CloseableHttpResponse get(String url) throws Exception {
        HttpUriRequest request = new HttpGet(url);
        return execute(request);
    }


    public CloseableHttpResponse execute(HttpUriRequest request) throws Exception {
        CloseableHttpResponse resp = null;
        if (isSecurity) {
            resp = doSecurityExecute(request);
        } else {
            resp = client.execute(request);
        }
        return resp;
    }

    public CloseableHttpResponse doSecurityExecute(HttpUriRequest request) {
        synchronized (this) {
            return Subject.doAs(serviceSubject, new PrivilegedAction<CloseableHttpResponse>() {
                CloseableHttpResponse httpResponse = null;

                @Override
                public CloseableHttpResponse run() {
                    try {
                        httpResponse = client.execute(request);
                        return httpResponse;
                    } catch (Exception ioe) {
                        LOG.error("url for " + request.getURI() + " failed ", ioe);
                        return null;
                    }
                }
            });
        }
    }

    public Subject getSubject() throws LoginException {
        javax.security.auth.login.Configuration config = new javax.security.auth.login.Configuration() {
            @SuppressWarnings("serial")
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[]{new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, new HashMap<String, Object>() {
                    {
                        put("useTicketCache", "false");
                        put("useKeyTab", "true");
                        put("keyTab", keyTabLocation);
                        put("refreshKrb5Config", "true");
                        put("principal", principal);
                        put("storeKey", "true");
                        put("doNotPrompt", "true");
                        put("isInitiator", "true");
                    }
                })};
            }
        };
        Set<Principal> princ = new HashSet<Principal>(1);
        princ.add(new KerberosPrincipal(principal));
        Subject sub = new Subject(false, princ, new HashSet<Object>(), new HashSet<Object>());
        LoginContext lc = new LoginContext("Krb5Login", sub, null, config);
        lc.login();

        Subject serviceSubject = lc.getSubject();
        return serviceSubject;
    }

    public void close() {
        try {
            if (null != client) {
                client.close();
            }
        } catch (Exception e) {

        }
    }


    class KdcRenewer extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(3 * 60 * 60 * 1000);
                } catch (Exception e) {
                    LOG.error("sleep error", e);
                }
                synchronized (HttpClient.this) {
                    try {
                        serviceSubject = getSubject();
                        LOG.info("renew KDC...");
                    } catch (Exception e) {
                        LOG.error("login error for ", e);
                    }

                }
            }

        }
    }
}