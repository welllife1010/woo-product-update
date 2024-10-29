import { AxiosRequestConfig } from "axios";
import OAuth from "oauth-1.0a";
import { WooRestApiMethod, IWooRestApiOptions, WooRestApiEndpoint, OrdersMainParams, ProductsMainParams, SystemStatusParams, CouponsParams, CustomersParams, DELETE } from "./typesANDinterfaces.js";
export { WooRestApiMethod, IWooRestApiQuery, IWooRestApiOptions, WooRestApiEndpoint, OrdersMainParams, ProductsMainParams, SystemStatusParams, CouponsParams, CustomersParams, DELETE, } from "./typesANDinterfaces.js";
export type WooRestApiOptions = IWooRestApiOptions<AxiosRequestConfig>;
export type WooRestApiParams = CouponsParams & CustomersParams & OrdersMainParams & ProductsMainParams & SystemStatusParams & DELETE;
export default class WooCommerceRestApi<T extends WooRestApiOptions> {
    protected _opt: T;
    constructor(opt: T);
    _setDefaultsOptions(opt: T): void;
    _normalizeQueryString(url: string, params: Partial<Record<string, any>>): string;
    _getUrl(endpoint: string, params: Partial<Record<string, unknown>>): string;
    _getOAuth(): OAuth;
    _request(method: WooRestApiMethod, endpoint: string, data?: Record<string, unknown>, params?: Record<string, unknown>): Promise<any>;
    get<T extends WooRestApiEndpoint>(endpoint: T, params?: Partial<WooRestApiParams>): Promise<any>;
    post<T extends WooRestApiEndpoint>(endpoint: T, data: Record<string, unknown>, params?: Partial<WooRestApiParams>): Promise<any>;
    put<T extends WooRestApiEndpoint>(endpoint: T, data: Record<string, unknown>, params?: Partial<WooRestApiParams>): Promise<any>;
    delete<T extends WooRestApiEndpoint>(endpoint: T, data: Pick<WooRestApiParams, "force">, params: Pick<WooRestApiParams, "id">): Promise<any>;
    options<T extends WooRestApiEndpoint>(endpoint: T, params?: Partial<WooRestApiParams>): Promise<any>;
}
export declare class OptionsException {
    name: "Options Error";
    message: string;
    constructor(message: string);
}
//# sourceMappingURL=index.d.ts.map