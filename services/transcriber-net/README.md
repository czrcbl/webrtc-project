# Transcriber-net

Transcriber service in .NET.

# Roadmap

- Webhook to trigger logic on room creation
- Mint a user token for a hidden bot participant
- Subscribe to track events, filter out no audio tracks
- Start a `worker` for each audio task detected
    - For each audio track, forward the audio to a transcription API
    - Keep track of the text
- Have a general worker for summarizing the session content and provide other features, like sentiment analysis. 


- start with a console app, avoid the webhook if necessary, making the logic simmilar to current python implementation
- Add ASP.Net if necessary

