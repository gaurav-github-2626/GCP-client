"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
exports.GCPqueue = void 0;
require('dotenv').config();
var PubSub = require("@google-cloud/pubsub").PubSub;
var v1 = require("@google-cloud/pubsub").v1;
var projectId = 'speedy-crawler-278510';
var subscriptionName = 'user';
var GCPqueue = /** @class */ (function () {
    function GCPqueue() {
        this.pubsub = new PubSub();
        this.subClient = new v1.SubscriberClient();
    }
    GCPqueue.prototype.sendMessage = function (resource, body, attributes, delayInSeconds) {
        return __awaiter(this, void 0, void 0, function () {
            var attri, dataBuffer, processAfterDate, messageId, messageId, messageId, err_1;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 7, , 8]);
                        attri = {};
                        if (attributes) {
                            attri = Array.from(customAttributes).reduce(function (attri, _a) {
                                var _b;
                                var key = _a[0], value = _a[1];
                                return (Object.assign(attri, (_b = {}, _b[key] = value, _b)));
                            }, {});
                        }
                        body = JSON.stringify(body);
                        dataBuffer = Buffer.from(body);
                        processAfterDate = 0;
                        if (!delayInSeconds) return [3 /*break*/, 2];
                        processAfterDate = new Date().getTime() + delayInSeconds * 1000;
                        attri['timestamp'] = JSON.stringify(processAfterDate);
                        console.log(attri);
                        return [4 /*yield*/, this.pubsub.topic(resource).publish(dataBuffer, attri)];
                    case 1:
                        messageId = _a.sent();
                        return [3 /*break*/, 6];
                    case 2:
                        if (!attributes) return [3 /*break*/, 4];
                        return [4 /*yield*/, this.pubsub.topic(resource).publish(dataBuffer, attri)];
                    case 3:
                        messageId = _a.sent();
                        console.log(attributes);
                        return [3 /*break*/, 6];
                    case 4: return [4 /*yield*/, this.pubsub.topic(resource).publish(dataBuffer)];
                    case 5:
                        messageId = _a.sent();
                        _a.label = 6;
                    case 6: return [2 /*return*/, true];
                    case 7:
                        err_1 = _a.sent();
                        return [2 /*return*/, err_1];
                    case 8: return [2 /*return*/];
                }
            });
        });
    };
    GCPqueue.prototype.sendMessageWithAttribute = function (resource, body, attributes, delayInSeconds) {
        return __awaiter(this, void 0, void 0, function () {
            var dataBuffer, processAfterDate, messageId, messageId, err_2;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 5, , 6]);
                        body = JSON.stringify(body);
                        dataBuffer = Buffer.from(body);
                        if (!delayInSeconds) return [3 /*break*/, 2];
                        processAfterDate = new Date().getTime() + delayInSeconds * 1000;
                        attributes['timestamp'] = JSON.stringify(processAfterDate);
                        console.log(attributes);
                        return [4 /*yield*/, this.pubsub.topic(resource).publish(dataBuffer, attributes)];
                    case 1:
                        messageId = _a.sent();
                        return [3 /*break*/, 4];
                    case 2: return [4 /*yield*/, this.pubsub.topic(resource).publish(dataBuffer, attributes)];
                    case 3:
                        messageId = _a.sent();
                        _a.label = 4;
                    case 4: return [2 /*return*/, true];
                    case 5:
                        err_2 = _a.sent();
                        return [2 /*return*/, err_2];
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    GCPqueue.prototype.receiveMessages = function (resource, batchSize) {
        return __awaiter(this, void 0, void 0, function () {
            var formattedSubscription, value, request, response, results, _i, _a, message, ACKId, text, att, currTime, flag, curr, err_3;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        _b.trys.push([0, 2, , 3]);
                        formattedSubscription = this.subClient.subscriptionPath(projectId, resource);
                        value = 1;
                        if (batchSize) {
                            value = batchSize;
                        }
                        request = {
                            subscription: formattedSubscription,
                            maxMessages: value
                        };
                        return [4 /*yield*/, this.subClient.pull(request)];
                    case 1:
                        response = (_b.sent())[0];
                        console.log(response);
                        results = [];
                        for (_i = 0, _a = response.receivedMessages; _i < _a.length; _i++) {
                            message = _a[_i];
                            ACKId = message.ackId;
                            text = message.message.data;
                            console.log("" + text);
                            att = message.message.attributes;
                            currTime = new Date().getTime();
                            flag = true;
                            if (att['timestamp'] && JSON.parse(att['timestamp']) > currTime) {
                                flag = false;
                            }
                            if (att['timestamp']) {
                                delete att.timestamp;
                            }
                            curr = {
                                data: text,
                                handle: ACKId,
                                attributes: att
                            };
                            if (flag == true) {
                                results.push(curr);
                            }
                        }
                        console.log(results.length);
                        return [2 /*return*/, results];
                    case 2:
                        err_3 = _b.sent();
                        return [2 /*return*/, err_3];
                    case 3: return [2 /*return*/];
                }
            });
        });
    };
    GCPqueue.prototype.deleteMessage = function (resource, handle) {
        return __awaiter(this, void 0, void 0, function () {
            var ackId, formattedSubscription, ackRequest, err_4;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        ackId = [];
                        ackId.push(handle);
                        formattedSubscription = this.subClient.subscriptionPath(projectId, resource);
                        ackRequest = {
                            subscription: formattedSubscription,
                            ackIds: ackId
                        };
                        return [4 /*yield*/, this.subClient.acknowledge(ackRequest)];
                    case 1:
                        _a.sent();
                        //console.log(handle);
                        console.log('Done.');
                        return [2 /*return*/, true];
                    case 2:
                        err_4 = _a.sent();
                        return [2 /*return*/, false];
                    case 3: return [2 /*return*/];
                }
            });
        });
    };
    return GCPqueue;
}());
exports.GCPqueue = GCPqueue;
var user = new GCPqueue();
var customAttributes = new Map();
customAttributes.set('origin', 'nodejs-sample');
customAttributes.set('username', 'gcp-1st-send');
// for(var i=0 ; i<7 ; i++){
//     user.sendMessage('test','This is a message from GCPQueue with deleted attribute of timestamp '+i,customAttributes,2).then((value) => {
//         console.log("message Sent !")
//     }).catch((err) => {
//         console.log(err);
//     });
// }
// const customAttributes = {
//     origin: 'nodejs-sample',
//     username: 'gcp-2nd-send',
// };
// user.sendMessageWithAttribute('test','this is a custom attribute message from GCPQueue',customAttributes).then((value) => {
//     console.log("Message Sent");
// }).catch((err)=>{
//     console.log(err);
// });
user.receiveMessages('user', 7).then(function (results) {
    for (var _i = 0, results_1 = results; _i < results_1.length; _i++) {
        var result = results_1[_i];
        if (result.handle == null) {
            console.log("No messages.");
        }
        else {
            console.log("" + result.data);
            console.log(result.attributes);
            user.deleteMessage('user', result.handle).then(function (value) {
                console.log("message Acknowledged !");
            })["catch"](function (err) {
                console.log("No acknowledgements");
            });
        }
    }
})["catch"](function (err) {
    console.log("There is some error with messages.");
    console.log(err);
});
