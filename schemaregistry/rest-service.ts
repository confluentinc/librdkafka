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

export interface BearerAuthCredentials {
  clientId: string,
  clientSecret: string,
  tokenHost: string,
  tokenPath: string,
  schemaRegistryLogicalCluster: string,
  identityPool: string,
  scope: string
}

//TODO: Consider retry policy, may need additional libraries on top of Axios
export interface ClientConfig {
  baseURLs: string[],
  cacheCapacity?: number,
  cacheLatestTtlSecs?: number,
  isForward?: boolean,
  createAxiosDefaults?: CreateAxiosDefaults,
  bearerAuthCredentials?: BearerAuthCredentials,
}

export class RestService {
  private client: AxiosInstance;
  private baseURLs: string[];
  private OAuthClient?: OAuthClient;
  private bearerAuth: boolean = false;

  constructor(baseURLs: string[], isForward?: boolean, axiosDefaults?: CreateAxiosDefaults,
    bearerAuthCredentials?: BearerAuthCredentials) {
    this.client = axios.create(axiosDefaults);
    this.baseURLs = baseURLs;

    if (isForward) {
      this.client.defaults.headers.common['X-Forward'] = 'true'
    }

    if (bearerAuthCredentials) {
      this.bearerAuth = true;
      delete this.client.defaults.auth;
      this.setHeaders({
        'Confluent-Identity-Pool-Id': bearerAuthCredentials.identityPool,
        'target-sr-cluster': bearerAuthCredentials.schemaRegistryLogicalCluster
      });
      this.OAuthClient = new OAuthClient(bearerAuthCredentials.clientId, bearerAuthCredentials.clientSecret,
        bearerAuthCredentials.tokenHost, bearerAuthCredentials.tokenPath, bearerAuthCredentials.scope);
    }
  }

  async handleRequest<T>(
    url: string,
    method: 'GET' | 'POST' | 'PUT' | 'DELETE',
    data?: any, // eslint-disable-line @typescript-eslint/no-explicit-any
    config?: AxiosRequestConfig,
  ): Promise<AxiosResponse<T>> {

    if (this.bearerAuth) {
      await this.setBearerToken();
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

  async setBearerToken(): Promise<void> {
    if (!this.OAuthClient) {
      throw new Error('OAuthClient not initialized');
    }

    const bearerToken: string = await this.OAuthClient.getAccessToken();
    this.setAuth(undefined, bearerToken);
  }

  setTimeout(timeout: number): void {
    this.client.defaults.timeout = timeout
  }

  setBaseURL(baseUrl: string): void {
    this.client.defaults.baseURL = baseUrl
  }
}
