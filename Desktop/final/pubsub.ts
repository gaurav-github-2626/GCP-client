import { IQueueService } from "./interface"
import { Message } from "./interface"

require('dotenv').config();
const {PubSub} = require(`@google-cloud/pubsub`);
const {v1} = require(`@google-cloud/pubsub`);


const projectId = 'speedy-crawler-278510';
const subscriptionName = 'user';

export class GCPqueue implements IQueueService{

    private pubsub;
    private subClient;
    
    constructor(
        
    ){
        this.pubsub = new PubSub();
        this.subClient = new v1.SubscriberClient();
    }
    
    public async sendMessage(resource: string, body: any, attributes?: {}, delayInSeconds?: number): Promise<boolean>{
        try{
            body = JSON.stringify(body);
            const dataBuffer = Buffer.from(body);
            var processAfterDate = 0
            if(delayInSeconds){
                processAfterDate = new Date().getTime() + delayInSeconds*1000
                attributes['timestamp'] = JSON.stringify(processAfterDate)
                console.log(attributes)
                const messageId = await this.pubsub.topic(resource).publish(dataBuffer,attributes)
            }else{
                if(attributes){
                    const messageId = await this.pubsub.topic(resource).publish(dataBuffer,attributes)
                    console.log(attributes)
                }else{
                    const messageId = await this.pubsub.topic(resource).publish(dataBuffer)
                }
            }
            return true;
        } catch(err){
            return err;
        }
    }

    public async sendMessageWithAttribute(resource: string, body: any, attributes: {}, delayInSeconds?: number): Promise<boolean>{
        try{
            body = JSON.stringify(body);
            const dataBuffer = Buffer.from(body);
            if(delayInSeconds){
                const processAfterDate = new Date().getTime() + delayInSeconds*1000
                attributes['timestamp'] = JSON.stringify(processAfterDate)
                console.log(attributes)
                const messageId = await this.pubsub.topic(resource).publish(dataBuffer,attributes)
            }else{
                const messageId = await this.pubsub.topic(resource).publish(dataBuffer,attributes)
            }
            return true;
        } catch(err){
            return err;
        }
    }

    public async receiveMessages(resource: string, batchSize?: number): Promise<Message[]>{
            try{
                //const projectId = 'speedy-crawler-278510';
                const formattedSubscription = this.subClient.subscriptionPath(
                    projectId,
                    resource
                );
                
                var value = 1
                if(batchSize){
                    value = batchSize
                }
                const request = {
                    subscription: formattedSubscription,
                    maxMessages: value,
                };
                const [response] = await this.subClient.pull(request)
                console.log(response);
                const results = [];
                for(const message of response.receivedMessages){
                    const ACKId : string = message.ackId;
                    const text : string = message.message.data;
                    console.log(`${text}`);
                    const att = message.message.attributes;
                    //console.log(att);
                    const currTime = new Date().getTime()
                    var flag = true
                    if(att['timestamp'] && JSON.parse(att['timestamp']) > currTime){
                        flag = false
                    }
                    if(att['timestamp']){
                        delete att.timestamp
                    }
                    const curr = {
                        data : text,
                        handle : ACKId,
                        attributes : att
                    };
                    if(flag == true){
                        results.push(curr);
                    }
                }
                console.log(results.length);
                return results;
            } catch(err){
                return err;
            }
    
    }

    public async deleteMessage(resource: string, handle: string): Promise<boolean> {
        try{
            const ackId = []
            ackId.push(handle)
            const formattedSubscription = this.subClient.subscriptionPath(
                projectId,
                resource
            );

             const ackRequest = {
                subscription: formattedSubscription,
                ackIds: ackId,
              };
              await this.subClient.acknowledge(ackRequest);
              //console.log(handle);
              console.log('Done.');

            return true;
        } catch(err){
            return false;
        }
    }
}



const user = new GCPqueue();

const customAttributes = {
    origin: 'nodejs-sample',
    username: 'gcp-1st-send',
};

for(var i=0 ; i<7 ; i++){
    user.sendMessage('test','This is a message from GCPQueue with deleted attribute of timestamp '+i,customAttributes,20).then((value) => {
        console.log("message Sent !")
    }).catch((err) => {
        console.log(err);
    });
}

// const customAttributes = {
//     origin: 'nodejs-sample',
//     username: 'gcp-2nd-send',
// };

// user.sendMessageWithAttribute('test','this is a custom attribute message from GCPQueue',customAttributes).then((value) => {
//     console.log("Message Sent");
// }).catch((err)=>{
//     console.log(err);
// });


user.receiveMessages('user',7).then((results) => { 
    for(const result of results){
        if(result.handle==null){
            console.log("No messages.");
        }else{
            console.log(`${result.data}`);
            console.log(result.attributes);
            user.deleteMessage('user',result.handle).then((value) => {
                console.log("message Acknowledged !");
            }).catch((err)=>{
                console.log("No acknowledgements");
            })
        }
    }
})
.catch((err) => {
    console.log("There is some error with messages.");
    console.log(err);
});




