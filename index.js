import Fastify from 'fastify';
import WebSocket from 'ws';
import fs from 'fs';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';
import fetch from 'node-fetch';

// Load environment variables from .env file
const dotenvResult = dotenv.config({ override: true });

// Retrieve the OpenAI API key from environment variables
const OPENAI_API_KEY = process.env.OPENAI_API_KEY?.trim();

if (!OPENAI_API_KEY) {
    console.error('Missing OpenAI API key. Please set it in the .env file.');
    process.exit(1);
}

if (!OPENAI_API_KEY.startsWith('sk-') || OPENAI_API_KEY.length < 40) {
    console.error('Invalid-looking OpenAI API key. Set OPENAI_API_KEY to a real key from https://platform.openai.com/api-keys.');
    process.exit(1);
}

console.log(`Loaded OpenAI API key ${OPENAI_API_KEY.slice(0, 7)}...${OPENAI_API_KEY.slice(-4)} (${OPENAI_API_KEY.length} chars)`);
console.log(`OpenAI API key source: ${dotenvResult.parsed?.OPENAI_API_KEY ? '.env file' : 'process environment'}`);
console.log(`Working directory: ${process.cwd()}`);

// Initialize Fastify
const fastify = Fastify();
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

// Constants
const SYSTEM_MESSAGE = 'You are an AI receptionist for Barts Automotive. Your job is to politely engage with the client and obtain their name, availability, and service/work required. Ask one question at a time. Do not ask for other contact information, and do not check availability, assume we are free. Ensure the conversation remains friendly and professional, and guide the user to provide these details naturally. If necessary, ask follow-up questions to gather the required information.';
const INITIAL_GREETING = 'Greet the caller as Bart\'s Automotive and ask how you can help today. Keep it brief.';
const VOICE = 'alloy';
const REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || 'gpt-realtime';
const PORT = process.env.PORT || 5050;
const WEBHOOK_URL = "<input your webhook URL here>";

// Session management
const sessions = new Map();

// List of Event Types to log to the console
const LOG_EVENT_TYPES = [
    'response.content.done',
    'rate_limits.updated',
    'response.done',
    'input_audio_buffer.committed',
    'input_audio_buffer.speech_stopped',
    'input_audio_buffer.speech_started',
    'session.created',
    'session.updated',
    'error',
    'response.created',
    'response.done',
    'response.text.done',
    'conversation.item.input_audio_transcription.completed',
    'response.output_audio_transcript.done'
];

// Root Route
fastify.get('/', async (request, reply) => {
    reply.send({ message: 'Twilio Media Stream Server is running!' });
});

// Route for Twilio to handle incoming and outgoing calls
fastify.all('/incoming-call', async (request, reply) => {
    console.log('Incoming call');

    const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
                          <Response>
                              <Say>Hi, you have called Bart's Automative Centre. How can we help?</Say>
                              <Connect>
                                  <Stream url="wss://${request.headers.host}/media-stream" />
                              </Connect>
                          </Response>`;

    reply.type('text/xml').send(twimlResponse);
});

// WebSocket route for media-stream
fastify.register(async (fastify) => {
    fastify.get('/media-stream', { websocket: true }, (connection, req) => {
        console.log('Client connected');

        const sessionId = req.headers['x-twilio-call-sid'] || `session_${Date.now()}`;
        let session = sessions.get(sessionId) || { transcript: '', streamSid: null };
        sessions.set(sessionId, session);

        const openAiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${REALTIME_MODEL}`, {
            headers: {
                Authorization: `Bearer ${OPENAI_API_KEY}`
            }
        });

        let sessionReady = false;
        let greetingSent = false;
        const pendingAudio = [];

        const sendSessionUpdate = () => {
            const sessionUpdate = {
                type: 'session.update',
                session: {
                    type: 'realtime',
                    instructions: SYSTEM_MESSAGE,
                    output_modalities: ["audio"],
                    temperature: 0.8,
                    audio: {
                        input: {
                            format: { type: 'audio/pcmu' },
                            turn_detection: {
                                type: 'server_vad',
                                create_response: true,
                                interrupt_response: true
                            },
                            transcription: {
                                model: 'whisper-1'
                            }
                        },
                        output: {
                            format: { type: 'audio/pcmu' },
                            voice: VOICE
                        }
                    }
                }
            };

            console.log('Sending session update:', JSON.stringify(sessionUpdate));
            openAiWs.send(JSON.stringify(sessionUpdate));
        };

        const sendInitialGreeting = () => {
            if (greetingSent || openAiWs.readyState !== WebSocket.OPEN) return;

            greetingSent = true;
            openAiWs.send(JSON.stringify({
                type: 'response.create',
                response: {
                    output_modalities: ['audio'],
                    instructions: INITIAL_GREETING
                }
            }));
        };

        // Open event for OpenAI WebSocket
        openAiWs.on('open', () => {
            console.log('Connected to the OpenAI Realtime API');
            sendSessionUpdate();
        });

        // Listen for messages from the OpenAI WebSocket
        openAiWs.on('message', (data) => {
            try {
                const response = JSON.parse(data);

                if (LOG_EVENT_TYPES.includes(response.type)) {
                    console.log(`Received event: ${response.type}`, response);
                }

                if (response.type === 'error') {
                    console.error('Realtime API error:', JSON.stringify(response.error, null, 2));
                }

                // User message transcription handling
                if (response.type === 'conversation.item.input_audio_transcription.completed') {
                    const userMessage = response.transcript.trim();
                    session.transcript += `User: ${userMessage}\n`;
                    console.log(`User (${sessionId}): ${userMessage}`);
                }

                // Agent message handling
                if (response.type === 'response.done') {
                    const agentMessage = response.response.output[0]?.content?.find(content => content.transcript)?.transcript;

                    if (agentMessage && !session.transcript.endsWith(`Agent: ${agentMessage}\n`)) {
                        session.transcript += `Agent: ${agentMessage}\n`;
                        console.log(`Agent (${sessionId}): ${agentMessage}`);
                    }
                }

                if (response.type === 'session.updated') {
                    sessionReady = true;
                    console.log('Session updated successfully');

                    while (pendingAudio.length > 0 && openAiWs.readyState === WebSocket.OPEN) {
                        openAiWs.send(JSON.stringify(pendingAudio.shift()));
                    }

                    sendInitialGreeting();
                }

                if ((response.type === 'response.output_audio.delta' || response.type === 'response.audio.delta') && response.delta) {
                    const audioDelta = {
                        event: 'media',
                        streamSid: session.streamSid,
                        media: { payload: response.delta }
                    };
                    connection.send(JSON.stringify(audioDelta));
                }

                if (response.type === 'response.output_audio_transcript.done' && response.transcript) {
                    session.transcript += `Agent: ${response.transcript}\n`;
                    console.log(`Agent (${sessionId}): ${response.transcript}`);
                }
            } catch (error) {
                console.error('Error processing OpenAI message:', error, 'Raw message:', data);
            }
        });

        // Handle incoming messages from Twilio
        connection.on('message', (message) => {
            try {
                const data = JSON.parse(message);

                switch (data.event) {
                    case 'media':
                        const audioAppend = {
                            type: 'input_audio_buffer.append',
                            audio: data.media.payload
                        };

                        if (openAiWs.readyState === WebSocket.OPEN && sessionReady) {
                            openAiWs.send(JSON.stringify(audioAppend));
                        } else if (openAiWs.readyState === WebSocket.CONNECTING || openAiWs.readyState === WebSocket.OPEN) {
                            pendingAudio.push(audioAppend);
                        }
                        break;
                    case 'start':
                        session.streamSid = data.start.streamSid;
                        console.log('Incoming stream has started', session.streamSid);
                        break;
                    default:
                        console.log('Received non-media event:', data.event);
                        break;
                }
            } catch (error) {
                console.error('Error parsing message:', error, 'Message:', message);
            }
        });

        // Handle connection close and log transcript
        connection.on('close', async () => {
            if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close();
            console.log(`Client disconnected (${sessionId}).`);
            console.log('Full Transcript:');
            console.log(session.transcript);

            await processTranscriptAndSend(session.transcript, sessionId);

            // Clean up the session
            sessions.delete(sessionId);
        });

        // Handle WebSocket close and errors
        openAiWs.on('close', (code, reason) => {
            console.log(`Disconnected from the OpenAI Realtime API (code ${code}${reason ? `, reason: ${reason}` : ''})`);
        });

        openAiWs.on('error', (error) => {
            console.error('Error in the OpenAI WebSocket:', error);
        });
    });
});

fastify.listen({ port: PORT }, (err) => {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(`Server is listening on port ${PORT}`);
});

// Function to make ChatGPT API completion call with structured outputs
async function makeChatGPTCompletion(transcript) {
    console.log('Starting ChatGPT API call...');
    try {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${OPENAI_API_KEY}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                model: "gpt-4o-2024-08-06",
                messages: [
                    { "role": "system", "content": "Extract customer details: name, availability, and any special notes from the transcript." },
                    { "role": "user", "content": transcript }
                ],
                response_format: {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "customer_details_extraction",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "customerName": { "type": "string" },
                                "customerAvailability": { "type": "string" },
                                "specialNotes": { "type": "string" }
                            },
                            "required": ["customerName", "customerAvailability", "specialNotes"]
                        }
                    }
                }
            })
        });

        console.log('ChatGPT API response status:', response.status);
        const data = await response.json();
        console.log('Full ChatGPT API response:', JSON.stringify(data, null, 2));
        return data;
    } catch (error) {
        console.error('Error making ChatGPT completion call:', error);
        throw error;
    }
}

// Function to send data to Make.com webhook
async function sendToWebhook(payload) {
    console.log('Sending data to webhook:', JSON.stringify(payload, null, 2));
    try {
        const response = await fetch(WEBHOOK_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        console.log('Webhook response status:', response.status);
        if (response.ok) {
            console.log('Data successfully sent to webhook.');
        } else {
            console.error('Failed to send data to webhook:', response.statusText);
        }
    } catch (error) {
        console.error('Error sending data to webhook:', error);
    }
}

// Main function to extract and send customer details
async function processTranscriptAndSend(transcript, sessionId = null) {
    console.log(`Starting transcript processing for session ${sessionId}...`);

    if (!transcript.trim()) {
        console.log('Transcript is empty; skipping customer detail extraction.');
        return;
    }

    try {
        // Make the ChatGPT completion call
        const result = await makeChatGPTCompletion(transcript);

        console.log('Raw result from ChatGPT:', JSON.stringify(result, null, 2));

        if (result.choices && result.choices[0] && result.choices[0].message && result.choices[0].message.content) {
            try {
                const parsedContent = JSON.parse(result.choices[0].message.content);
                console.log('Parsed content:', JSON.stringify(parsedContent, null, 2));

                if (parsedContent) {
                    // Send the parsed content directly to the webhook
                    await sendToWebhook(parsedContent);
                    console.log('Extracted and sent customer details:', parsedContent);
                } else {
                    console.error('Unexpected JSON structure in ChatGPT response');
                }
            } catch (parseError) {
                console.error('Error parsing JSON from ChatGPT response:', parseError);
            }
        } else {
            console.error('Unexpected response structure from ChatGPT API');
        }

    } catch (error) {
        console.error('Error in processTranscriptAndSend:', error);
    }
}
