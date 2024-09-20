import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse, CreateAxiosDefaults } from 'axios';
import { OAuthClient } from './oauth/oauth-client';
import { RestError } from './rest-error';
/*
 * Confluent-Schema-Registry-TypeScript - Node.js wrapper for Confluent Schema Registry
 *
 * Copyright (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

export interface BasicAuthCredentials {
  credentialsSource: 'USER_INFO' | 'URL' | 'SASL_INHERIT',
  userInfo?: string,
  saslInfo?: SaslInfo
}

export interface SaslInfo {
  mechanism?: string,
  username: string,
  password: string
}

export interface BearerAuthCredentials {
  credentialsSource: 'STATIC_TOKEN' | 'OAUTHBEARER',
  token?: string,
  issuerEndpointUrl?: string,
  clientId?: string,
  clientSecret?: string,
  scope?: string,
  logicalCluster?: string,
  identityPoolId?: string,
}

//TODO: Consider retry policy, may need additional libraries on top of Axios
export interface ClientConfig {
  baseURLs: string[],
  cacheCapacity?: number,
  cacheLatestTtlSecs?: number,
  isForward?: boolean,
  createAxiosDefaults?: CreateAxiosDefaults,
  basicAuthCredentials?: BasicAuthCredentials,
  bearerAuthCredentials?: BearerAuthCredentials,
}

const toBase64 = (str: string): string => Buffer.from(str).toString('base64');

export class RestService {
  private client: AxiosInstance;
  private baseURLs: string[];
  private oauthClient?: OAuthClient;
  private oauthBearer: boolean = false;

  constructor(baseURLs: string[], isForward?: boolean, axiosDefaults?: CreateAxiosDefaults,
    basicAuthCredentials?: BasicAuthCredentials, bearerAuthCredentials?: BearerAuthCredentials) {
    this.client = axios.create(axiosDefaults);
    this.baseURLs = baseURLs;

    if (isForward) {
      this.client.defaults.headers.common['X-Forward'] = 'true'
    }

    this.handleBasicAuth(basicAuthCredentials);
    this.handleBearerAuth(bearerAuthCredentials);

    if (!basicAuthCredentials && !bearerAuthCredentials) {
      throw new Error('No auth credentials provided');
    }
  }

  handleBasicAuth(basicAuthCredentials?: BasicAuthCredentials): void {
    if (basicAuthCredentials) {
      switch (basicAuthCredentials.credentialsSource) {
        case 'USER_INFO':
          if (!basicAuthCredentials.userInfo) {
            throw new Error('User info not provided');
          }
          this.setAuth(toBase64(basicAuthCredentials.userInfo!));
          break;
        case 'SASL_INHERIT':
          if (!basicAuthCredentials.saslInfo) {
            throw new Error('Sasl info not provided');
          }
          if (basicAuthCredentials.saslInfo.mechanism?.toUpperCase() === 'GSSAPI') {
            throw new Error('SASL_INHERIT support PLAIN and SCRAM SASL mechanisms only');
          }
          this.setAuth(toBase64(`${basicAuthCredentials.saslInfo.username}:${basicAuthCredentials.saslInfo.password}`));
          break;
        case 'URL':
          if (!basicAuthCredentials.userInfo) {
            throw new Error('User info not provided');
          }
          const basicAuthUrl = new URL(basicAuthCredentials.userInfo);
          this.setAuth(toBase64(`${basicAuthUrl.username}:${basicAuthUrl.password}`));
          break;
        default:
          throw new Error('Invalid basic auth credentials source');
      }
    }
  }

  handleBearerAuth(bearerAuthCredentials?: BearerAuthCredentials): void {
    if (bearerAuthCredentials) {
      delete this.client.defaults.auth;

      const headers = ['logicalCluster', 'identityPoolId'];
      const missingHeaders = headers.find(header => bearerAuthCredentials[header as keyof typeof bearerAuthCredentials]);

      if (missingHeaders) {
        throw new Error(`Bearer auth header '${missingHeaders}' not provided`);
      }

      this.setHeaders({
        'Confluent-Identity-Pool-Id': bearerAuthCredentials.identityPoolId!,
        'target-sr-cluster': bearerAuthCredentials.logicalCluster!
      });

      switch (bearerAuthCredentials.credentialsSource) {
        case 'STATIC_TOKEN':
          if (!bearerAuthCredentials.token) {
            throw new Error('Bearer token not provided');
          }
          this.setAuth(undefined, bearerAuthCredentials.token);
          break;
        case 'OAUTHBEARER':
          this.oauthBearer = true;
          const requiredFields = [
            'clientId',
            'clientSecret',
            'issuerEndpointUrl',
            'scope'
          ];
          const missingField = requiredFields.find(field => bearerAuthCredentials[field as keyof typeof bearerAuthCredentials]);

          if (missingField) {
            throw new Error(`OAuth credential '${missingField}' not provided`);
          }
          const issuerEndPointUrl = new URL(bearerAuthCredentials.issuerEndpointUrl!);
          this.oauthClient = new OAuthClient(bearerAuthCredentials.clientId!, bearerAuthCredentials.clientSecret!,
            issuerEndPointUrl.host, issuerEndPointUrl.pathname, bearerAuthCredentials.scope!);
          break;
        default:
          throw new Error('Invalid bearer auth credentials source');
      }
    }
  }

  async handleRequest<T>(
    url: string,
    method: 'GET' | 'POST' | 'PUT' | 'DELETE',
    data?: any, // eslint-disable-line @typescript-eslint/no-explicit-any
    config?: AxiosRequestConfig,
  ): Promise<AxiosResponse<T>> {

    if (this.oauthBearer) {
      await this.setOAuthBearerToken();
    }

    for (let i = 0; i < this.baseURLs.length; i++) {
      try {
        this.setBaseURL(this.baseURLs[i]);
        const response = await this.client.request<T>({
          url,
          method,
          data,
          ...config,
        })
        return response;
      } catch (error) {
        if (axios.isAxiosError(error) && error.response && (error.response.status < 200 || error.response.status > 299)) {
          const data = error.response.data;
          if (data.error_code && data.message) {
            error = new RestError(data.message, error.response.status, data.error_code);
          } else {
            error = new Error(`Unknown error: ${error.message}`)
          }
        }
        if (i === this.baseURLs.length - 1) {
          throw error;
        }
      }
    }

    throw new Error('Internal HTTP retry error'); // Should never reach here
  }

  setHeaders(headers: Record<string, string>): void {
    this.client.defaults.headers.common = { ...this.client.defaults.headers.common, ...headers }
  }

  setAuth(basicAuth?: string, bearerToken?: string): void {
    if (basicAuth) {
      this.client.defaults.headers.common['Authorization'] = `Basic ${basicAuth}`
    }

    if (bearerToken) {
      this.client.defaults.headers.common['Authorization'] = `Bearer ${bearerToken}`
    }
  }

  async setOAuthBearerToken(): Promise<void> {
    if (!this.oauthClient) {
      throw new Error('OAuthClient not initialized');
    }

    const bearerToken: string = await this.oauthClient.getAccessToken();
    this.setAuth(undefined, bearerToken);
  }

  setTimeout(timeout: number): void {
    this.client.defaults.timeout = timeout
  }

  setBaseURL(baseUrl: string): void {
    this.client.defaults.baseURL = baseUrl
  }
}
