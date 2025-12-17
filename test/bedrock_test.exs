defmodule Instructor.Adapters.BedrockTest do
  use ExUnit.Case, async: true

  alias Instructor.Adapters.Bedrock

  describe "build_converse_body_for_test/4" do
    test "text-only messages remain unchanged" do
      assert {:ok, body} =
               Bedrock.build_converse_body_for_test(
                 [%{role: "user", content: "hello"}],
                 10,
                 1.0,
                 []
               )

      assert %{
               "messages" => [
                 %{
                   "role" => "user",
                   "content" => [%{"text" => "hello"}]
                 }
               ]
             } = body
    end

    test "user message with mixed text + image blocks formats correctly" do
      assert {:ok, body} =
               Bedrock.build_converse_body_for_test(
                 [
                   %{
                     role: "user",
                     content: [
                       %{type: "text", text: "what is in this image?"},
                       %{type: "image", format: "png", data: "aGVsbG8="}
                     ]
                   }
                 ],
                 10,
                 1.0,
                 []
               )

      assert %{
               "messages" => [
                 %{
                   "role" => "user",
                   "content" => [
                     %{"text" => "what is in this image?"},
                     %{"image" => %{"format" => "png", "source" => %{"bytes" => "aGVsbG8="}}}
                   ]
                 }
               ]
             } = body
    end

    test "image block with non-user role returns error" do
      assert {:error, reason} =
               Bedrock.build_converse_body_for_test(
                 [
                   %{
                     role: "assistant",
                     content: [%{type: "image", format: "png", data: "aGVsbG8="}]
                   }
                 ],
                 10,
                 1.0,
                 []
               )

      assert reason == "Bedrock image blocks are only supported for user messages."
    end

    test "unknown block returns error" do
      assert {:error, reason} =
               Bedrock.build_converse_body_for_test(
                 [%{role: "user", content: [%{type: "audio"}]}],
                 10,
                 1.0,
                 []
               )

      assert reason == "Unsupported Bedrock content block."
    end

    test "unsupported image format returns error" do
      assert {:error, reason} =
               Bedrock.build_converse_body_for_test(
                 [
                   %{
                     role: "user",
                     content: [%{type: "image", format: "tiff", data: "aGVsbG8="}]
                   }
                 ],
                 10,
                 1.0,
                 []
               )

      assert reason ==
               "Unsupported Bedrock image format \"tiff\". Supported formats: png, jpeg, gif, webp."
    end
  end
end
