defmodule Instructor.AWSEventStreamParserTest do
  use ExUnit.Case, async: true

  alias Instructor.AWSEventStreamParser

  # AWS Event Stream message structure (all fields are 32-bit / 4 bytes)
  @total_length_bytes 4
  @headers_length_bytes 4
  @prelude_crc_bytes 4
  @message_crc_bytes 4
  @prelude_bytes @total_length_bytes + @headers_length_bytes + @prelude_crc_bytes

  describe "parse/1" do
    test "parses a single event" do
      payload = %{"text" => "Lorem ipsum"}
      event_data = build_content_block_data(payload)

      result =
        event_data
        |> build_event_stream_message()
        |> List.wrap()
        |> Stream.concat([])
        |> AWSEventStreamParser.parse()
        |> Enum.to_list()

      assert [%{"contentBlockDelta" => %{"delta" => ^payload}}] = result
    end

    test "parses multiple events" do
      event1 = build_event_stream_message(%{"messageStart" => %{"role" => "assistant"}})

      event2 =
        build_content_block_data()
        |> build_event_stream_message()

      event3 = build_event_stream_message(%{"messageStop" => %{"stopReason" => "end_turn"}})

      result =
        [event1 <> event2 <> event3]
        |> Stream.concat([])
        |> AWSEventStreamParser.parse()
        |> Enum.to_list()

      assert length(result) == 3
      assert [%{"messageStart" => _}, %{"contentBlockDelta" => _}, %{"messageStop" => _}] = result
    end

    test "handles chunked data across multiple stream elements" do
      payload = %{"text" => "Lorem ipsum dolor sit amet"}

      event =
        build_content_block_data(payload)
        |> build_event_stream_message()

      {part1, part2} = String.split_at(event, div(byte_size(event), 2))

      result =
        [part1, part2]
        |> Stream.concat([])
        |> AWSEventStreamParser.parse()
        |> Enum.to_list()

      assert [%{"contentBlockDelta" => %{"delta" => ^payload}}] = result
    end

    test "parses tool use events" do
      event =
        build_content_block_data(%{
          "toolUse" => %{"input" => ~s|{name: test}|}
        })
        |> put_in(["contentBlockDelta", "delta", "contentBlockIndex"], 0)
        |> build_event_stream_message()

      result =
        [event]
        |> Stream.concat([])
        |> AWSEventStreamParser.parse()
        |> Enum.to_list()

      assert [%{"contentBlockDelta" => %{"delta" => %{"toolUse" => %{"input" => _}}}}] = result
    end
  end

  # Private Helper functions
  defp build_event_stream_message(payload) do
    json_payload = Jason.encode!(payload)

    headers =
      encode_headers([
        {":event-type", "chunk"},
        {":content-type", "application/json"},
        {":message-type", "event"}
      ])

    headers_length = byte_size(headers)
    payload_length = byte_size(json_payload)
    total_length = @prelude_bytes + headers_length + payload_length + @message_crc_bytes

    # Build the message (using 0 for CRCs since our parser ignores them)
    <<total_length::32, headers_length::32, 0::32>> <>
      headers <>
      json_payload <>
      <<0::32>>
  end

  # AWS Event Stream binary format encoding
  defp encode_headers(header_list) do
    Enum.reduce(header_list, <<>>, fn {name, value}, acc ->
      acc <>
        <<byte_size(name)::8>> <>
        name <>
        <<7::8>> <>
        <<byte_size(value)::16>> <>
        value
    end)
  end

  defp build_content_block_data(payload \\ %{"text" => "Hello World"}) do
    %{"contentBlockDelta" => %{"delta" => payload}}
  end
end
