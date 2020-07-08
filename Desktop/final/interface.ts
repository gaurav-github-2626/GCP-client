export interface Message {
    data: string
    handle : string
    attributes?: { [key: string]: any }
}

export interface IQueueService {
    sendMessage(resource: string, body: any, attributes?: Map<string,any>, delayInSeconds?: number): Promise<boolean>
    sendMessageWithAttribute(resource: string, body: any, attributes: {}, delayInSeconds?: number): Promise<boolean>
    receiveMessages(resource: string, batchSize?: number): Promise<Message[]>
    deleteMessage(resource: string, handle: string): Promise<boolean>
    //isTracingEnabled(): boolean
}