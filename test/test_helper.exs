Mox.defmock(InstructorTest.MockOpenAI, for: Instructor.Adapter)
Mox.defmock(InstructorTest.MockBedrock, for: Instructor.Adapter)

# Exclude the unmocked tests by default, to run them use:
#
#   mix test --only adapter:llamacpp
#   mix test --only adapter:openai
#   mix test --only adapter:bedrock
#
# to run all the non-local models, use:
#
#   mix test --include adapter:gemini --include adapter:anthropic --include adapter:openai --include adapter:bedrock
#
#
ExUnit.configure(
  exclude: [
    adapter: :openai,
    adapter: :groq,
    adapter: :anthropic,
    adapter: :gemini,
    adapter: :xai,
    adapter: :llamacpp,
    adapter: :ollama,
    adapter: :bedrock
  ]
)

ExUnit.start()
