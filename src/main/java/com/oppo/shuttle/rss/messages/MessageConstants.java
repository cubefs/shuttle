/*
 * Copyright 2021 OPPO. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.shuttle.rss.messages;

public class MessageConstants {
    public final static byte RESPONSE_STATUS_OK = 20;
    public final static byte RESPONSE_STATUS_SERVER_BUSY = 53;
    public final static byte RESPONSE_STATUS_INVALID_BUILD_CONNECTION_ID_VALUE = 60;
    public final static byte RESPONSE_STATUS_INVALID_PACKAGE = 61;

    // Data message
    public final static int MESSAGE_UploadPackageRequest = 10;

    public final static int MESSAGE_BuildConnectionRequest = -322;
    public final static int MESSAGE_BuildConnectionResponse = -323;
    public final static int MESSAGE_UploadPackageResponse = -326;

    // Master/worker control message
    public final static int MESSAGE_SHUFFLE_WORKER_REGISTER_REQUEST = -330;
    public final static int MESSAGE_SHUFFLE_WORKER_HEALTH_INFO = -331;
    public final static int MESSAGE_DRIVER_REQUEST_INFO = -332;
    public final static int MESSAGE_SHUFFLE_WORKER_REGISTER_RESPONSE = -333;
    public final static int MESSAGE_SHUFFLE_RESPONSE_INFO = -334;
    public final static int MESSAGE_SHUFFLE_WORKER_UNREGISTER_REQUEST = -334;

    // flow control message number
    public final static int FLOW_CONTROL_NONE = 101;      // no flow control
    public final static int FLOW_CONTROL_MEMORY = 102;
    public final static int FLOW_CONTROL_BUSY = 103;
    public final static int FLOW_CONTROL_BYPASS = 104;
}
