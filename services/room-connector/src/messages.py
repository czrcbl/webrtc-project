  from typing import Annotated, Literal
 
  from pydantic import BaseModel, Field, TypeAdapter
  
  # Binary frames: raw PCM, signed 16-bit little-endian, mono, at this rate.
  SAMPLE_RATE = 16_000


  # Client -> server
  
  class EofMessage(BaseModel):
      type: Literal["eof"] = "eof"

  
  # Server -> client

  class ReadyMessage(BaseModel):
      type: Literal["ready"] = "ready"
  

  class PartialMessage(BaseModel):
      """Uncommitted tail of the stream; replaced by the next partial or final."""
  
      type: Literal["partial"] = "partial"
      text: str

  class FinalMessage(BaseModel):
      """Committed segment. Times are seconds from the start of the stream."""
 
      type: Literal["final"] = "final"
      start: float
      end: float
      text: str
  

  class DoneMessage(BaseModel):
      type: Literal["done"] = "done"
      language: str | None
 
  
  class ErrorMessage(BaseModel):
      type: Literal["error"] = "error"
      message: str
  

  ServerMessage = Annotated[
      ReadyMessage | PartialMessage | FinalMessage | DoneMessage | ErrorMessage,
      Field(discriminator="type"),
  ]
  server_message = TypeAdapter(ServerMessage)