import {
  appendClientMessage,
  appendResponseMessages,
  createDataStream,
  smoothStream,
  streamText,
} from 'ai';
import { auth, type UserType } from '@/app/(auth)/auth';
import { type RequestHints, systemPrompt } from '@/lib/ai/prompts';
import {
  createStreamId,
  deleteChatById,
  getChatById,
  getMessageCountByUserId,
  getMessagesByChatId,
  getStreamIdsByChatId,
  saveChat,
  saveMessages,
} from '@/lib/db/queries';
import { generateUUID, getTrailingMessageId } from '@/lib/utils';
import { generateTitleFromUserMessage } from '../../actions';
import { createDocument } from '@/lib/ai/tools/create-document';
import { updateDocument } from '@/lib/ai/tools/update-document';
import { requestSuggestions } from '@/lib/ai/tools/request-suggestions';
import { getWeather } from '@/lib/ai/tools/get-weather';
import { isProductionEnvironment } from '@/lib/constants';
import { myProvider } from '@/lib/ai/providers';
import { entitlementsByUserType } from '@/lib/ai/entitlements';
import { postRequestBodySchema, type PostRequestBody } from './schema';
import { geolocation } from '@vercel/functions';
import {
  createResumableStreamContext,
  type ResumableStreamContext,
} from 'resumable-stream';
import { after } from 'next/server';
import type { Chat } from '@/lib/db/schema';
import { differenceInSeconds } from 'date-fns';
import { ChatSDKError } from '@/lib/errors';
import { S2 } from "@s2-dev/streamstore";
import { ReadAcceptEnum } from '@s2-dev/streamstore/sdk/records.js';
import { EventStream } from '@s2-dev/streamstore/lib/event-streams.js';
import { ReadBatch, ReadEvent } from '@s2-dev/streamstore/models/components';


const s2 = new S2({
  accessToken: "YAAAAAAAAABoKKKXemvUtmN+OKZfmM9pkUJEfLN03fR7/ymV",  
})

export const maxDuration = 60;

let globalStreamContext: ResumableStreamContext | null = null;

function getStreamContext() {
  if (!globalStreamContext) {
    try {
      globalStreamContext = createResumableStreamContext({
        waitUntil: after,
      });
    } catch (error: any) {
      if (error.message.includes('REDIS_URL')) {
        console.log(
          ' > Resumable streams are disabled due to missing REDIS_URL',
        );
      } else {
        console.error(error);
      }
    }
  }

  return globalStreamContext;
}

export async function POST(request: Request) {
  let requestBody: PostRequestBody;

  try {
    const json = await request.json();
    requestBody = postRequestBodySchema.parse(json);
  } catch (_) {
    return new ChatSDKError('bad_request:api').toResponse();
  }

  try {
    const { id, message, selectedChatModel, selectedVisibilityType } =
      requestBody;

    const session = await auth();

    if (!session?.user) {
      return new ChatSDKError('unauthorized:chat').toResponse();
    }

    const userType: UserType = session.user.type;

    const messageCount = await getMessageCountByUserId({
      id: session.user.id,
      differenceInHours: 24,
    });

    if (messageCount > entitlementsByUserType[userType].maxMessagesPerDay) {
      return new ChatSDKError('rate_limit:chat').toResponse();
    }

    const chat = await getChatById({ id });

    if (!chat) {
      const title = await generateTitleFromUserMessage({
        message,
      });

      await saveChat({
        id,
        userId: session.user.id,
        title,
        visibility: selectedVisibilityType,
      });
    } else {
      if (chat.userId !== session.user.id) {
        return new ChatSDKError('forbidden:chat').toResponse();
      }
    }

    const previousMessages = await getMessagesByChatId({ id });

    const messages = appendClientMessage({
      // @ts-expect-error: todo add type conversion from DBMessage[] to UIMessage[]
      messages: previousMessages,
      message,
    });

    const { longitude, latitude, city, country } = geolocation(request);

    const requestHints: RequestHints = {
      longitude,
      latitude,
      city,
      country,
    };

    await saveMessages({
      messages: [
        {
          chatId: id,
          id: message.id,
          role: 'user',
          parts: message.parts,
          attachments: message.experimental_attachments ?? [],
          createdAt: new Date(),
        },
      ],
    });

    const streamId = generateUUID();
    await createStreamId({ streamId, chatId: id });

    const stream = createDataStream({
      execute: (dataStream) => {
        const result = streamText({
          model: myProvider.languageModel(selectedChatModel),
          system: systemPrompt({ selectedChatModel, requestHints }),
          messages,
          maxSteps: 5,
          experimental_activeTools:
            selectedChatModel === 'chat-model-reasoning'
              ? []
              : [
                  'getWeather',
                  'createDocument',
                  'updateDocument',
                  'requestSuggestions',
                ],
          experimental_transform: smoothStream({ chunking: 'word' }),
          experimental_generateMessageId: generateUUID,
          tools: {
            getWeather,
            createDocument: createDocument({ session, dataStream }),
            updateDocument: updateDocument({ session, dataStream }),
            requestSuggestions: requestSuggestions({
              session,
              dataStream,
            }),
          },
          onFinish: async ({ response }) => {
            if (session.user?.id) {
              try {
                const assistantId = getTrailingMessageId({
                  messages: response.messages.filter(
                    (message) => message.role === 'assistant',
                  ),
                });

                if (!assistantId) {
                  throw new Error('No assistant message found!');
                }

                const [, assistantMessage] = appendResponseMessages({
                  messages: [message],
                  responseMessages: response.messages,
                });

                await saveMessages({
                  messages: [
                    {
                      id: assistantId,
                      chatId: id,
                      role: assistantMessage.role,
                      parts: assistantMessage.parts,
                      attachments:
                        assistantMessage.experimental_attachments ?? [],
                      createdAt: new Date(),
                    },
                  ],
                });
              } catch (_) {
                console.error('Failed to save chat');
              }
            }
          },
          experimental_telemetry: {
            isEnabled: isProductionEnvironment,
            functionId: 'stream-text',
          },
        });

        result.consumeStream();

        result.mergeIntoDataStream(dataStream, {
          sendReasoning: true,
        });
      },
      onError: () => {
        return 'Oops, an error occurred!';
      },
    });

    console.log('streamId', streamId);
    
    const [s2Stream, clientStream] = stream.tee();
    
    (async () => {
      const reader = s2Stream.getReader();
      const BATCH_SIZE = 10;
      let currentBatch: string[] = [];
      
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {            
            if (currentBatch.length > 0) {
              try {
                await s2.records.append({
                  stream: streamId,
                  appendInput: {
                    records: currentBatch.map(body => ({ body })),
                  },
                }, { serverURL: "https://mehul-teste.b.aws.s2.dev/v1" });
                console.log(`Appended final batch of ${currentBatch.length} records`);
              } catch (error) {
                console.error('Error appending final batch to S2:', error);
              }
            }
            // Append stream end marker
            await s2.records.append({
              stream: streamId,
              appendInput: {
                records: [{ body: "Stream ended" }],
              },
            }, { serverURL: "https://mehul-teste.b.aws.s2.dev/v1" });
            console.log('Stream ended marker appended');
            break;
          }

          // Add to current batch
          currentBatch.push(value);
          console.log(`Added to batch, current size: ${currentBatch.length}`);

          // If batch is full, append to S2
          if (currentBatch.length >= BATCH_SIZE) {
            try {
              await s2.records.append({
                stream: streamId,
                appendInput: {
                  records: currentBatch.map(body => ({ body })),
                },
              }, { serverURL: "https://mehul-teste.b.aws.s2.dev/v1" });
              console.log(`Appended batch of ${currentBatch.length} records to S2`);
              currentBatch = []; // Clear the batch after successful append
            } catch (error) {
              console.error('Error appending batch to S2:', error);
              // Continue with next batch even if this one fails
              currentBatch = [];
            }
          }
        }
      } catch (error) {
        console.error('Error in S2 append stream:', error);
      } finally {
        reader.releaseLock();
      }
    })();
    
    let abortController: AbortController;
    let isControllerClosed = false;
    const READ_TIMEOUT = 30000;

    const handleCancel = () => {
      console.log('Client stream cancelled');
      if (abortController) {
        abortController.abort();
      }
    };

    const clientReadableStream = new ReadableStream({
      async start(controller) {
        const reader = clientStream.getReader();
        abortController = new AbortController();
        const signal = abortController.signal;
        
        const streamDone = new Promise<void>((resolve) => {
          signal.addEventListener('abort', () => {
            console.log('Stream aborted');
            resolve();
          });
        });

        const closeController = () => {
          if (!isControllerClosed) {
            try {
              controller.close();
              isControllerClosed = true;
            } catch (error) {
              if (error instanceof Error && error.name !== 'TypeError') {
                console.error('Unexpected error while closing controller:', error);
              } else {
                console.log('Controller was already closed, no action needed.');
              }
            }
          }
        };

        const readWithTimeout = async () => {
          const timeoutPromise = new Promise<{ done: boolean; value: any }>((_, reject) => {
            setTimeout(() => {
              reject(new Error('Read timeout'));
            }, READ_TIMEOUT);
          });

          try {
            return await Promise.race([
              reader.read(),
              timeoutPromise
            ]);
          } catch (error) {
            if (error instanceof Error && error.message === 'Read timeout') {
              console.log('Read timeout occurred, aborting stream');
              abortController.abort();
              throw error;
            }
            throw error;
          }
        };

        try {
          while (!signal.aborted && !isControllerClosed) {
            let readResult;
            try {
              readResult = await readWithTimeout();
            } catch (error) {
              if (error instanceof Error && error.message === 'Read timeout') {                
                break;
              }
              throw error;
            }

            const { done, value } = readResult;
            
            if (done || signal.aborted) {
              closeController();
              break;
            }

            if (!signal.aborted && !isControllerClosed) {
              try {
                const encodedValue = new TextEncoder().encode(value);
                controller.enqueue(encodedValue);
              } catch (error) {
                console.error('Error enqueueing to client:', error);
                abortController.abort();
                closeController();
                break;
              }
            }
          }
        } catch (error) {
          console.error('Error in client stream:', error);
          if (!signal.aborted && !isControllerClosed) {
            try {
              controller.error(error);
              isControllerClosed = true;
            } catch (closeError) {
              console.error('Error sending error to client:', closeError);
            }
          }
        } finally {
          reader.releaseLock();
          closeController();
        }
        
        await streamDone;
      },
      cancel: handleCancel
    });

    return new Response(clientReadableStream);
  } catch (error) {
    if (error instanceof ChatSDKError) {
      return error.toResponse();
    }
  }
}

export async function GET(request: Request) {
  const streamContext = getStreamContext();
  const resumeRequestedAt = new Date();

  if (!streamContext) {
    return new Response(null, { status: 204 });
  }

  const { searchParams } = new URL(request.url);
  const chatId = searchParams.get('chatId');

  if (!chatId) {
    return new ChatSDKError('bad_request:api').toResponse();
  }

  const session = await auth();

  if (!session?.user) {
    return new ChatSDKError('unauthorized:chat').toResponse();
  }

  let chat: Chat;

  try {
    chat = await getChatById({ id: chatId });
  } catch {
    return new ChatSDKError('not_found:chat').toResponse();
  }

  if (!chat) {
    return new ChatSDKError('not_found:chat').toResponse();
  }

  if (chat.visibility === 'private' && chat.userId !== session.user.id) {
    return new ChatSDKError('forbidden:chat').toResponse();
  }

  const streamIds = await getStreamIdsByChatId({ chatId });

  if (!streamIds.length) {
    return new ChatSDKError('not_found:stream').toResponse();
  }

  const recentStreamId = streamIds.at(-1);

  if (!recentStreamId) {
    return new ChatSDKError('not_found:stream').toResponse();
  }

  const emptyDataStream = createDataStream({
    execute: () => {},
  });

  const stream = await streamContext.resumableStream(
    recentStreamId,
    () => emptyDataStream,
  );

 
  console.log('recentStreamId', recentStreamId);

  const s2Stream = new ReadableStream({
    async start(controller) {
      let isClosed = false;
      try {
        console.log('Starting to read from S2 stream:', recentStreamId);
        const records = await s2.records.read(
          {
            stream: recentStreamId,
            seqNum: 0,
          },
          {
            acceptHeaderOverride: ReadAcceptEnum.textEventStream,
            serverURL: "https://mehul-teste.b.aws.s2.dev/v1",
          }
        );

        const recordsStream = records as EventStream<ReadEvent>;
        let recordCount = 0;
        let batchCount = 0;

        for await (const record of recordsStream) {
          if (record.event !== 'batch') continue;

          const batch = record.data as ReadBatch;
          batchCount++;
          for (const rec of batch.records) {
            console.log('Processing record:', rec);
            if (isClosed) break;
            recordCount++;
            if (rec.body === 'Stream ended') {
              controller.close();
              isClosed = true;
              break;
            }
            if (rec.body) {
              try {
                if (isClosed) break;
                const encodedValue = new TextEncoder().encode(rec.body);
                controller.enqueue(encodedValue);
                console.log('Enqueued record:', rec.body);
                // sleep for 1 second
                // await new Promise(resolve => setTimeout(resolve, 10000));
                
              } catch (error) {
                if (!isClosed) {
                  console.error('Error processing record:', error);
                }
              }
            }
          }
          if (isClosed) break;
        }

        if (!isClosed) controller.close();
      } catch (error) {
        console.error('Error reading from S2 stream:', error);
        try {
          controller.error(error);
        } catch (closeError) {
          console.error('Error sending error to client:', closeError);
        }
      }
    }
  });

  return new Response(s2Stream, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    },
  });
}

export async function DELETE(request: Request) {
  const { searchParams } = new URL(request.url);
  const id = searchParams.get('id');

  if (!id) {
    return new ChatSDKError('bad_request:api').toResponse();
  }

  const session = await auth();

  if (!session?.user) {
    return new ChatSDKError('unauthorized:chat').toResponse();
  }

  const chat = await getChatById({ id });

  if (chat.userId !== session.user.id) {
    return new ChatSDKError('forbidden:chat').toResponse();
  }

  const deletedChat = await deleteChatById({ id });

  return Response.json(deletedChat, { status: 200 });
}