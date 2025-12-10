defmodule Instructor.Adapters.Bedrock do
  @moduledoc """
  AWS Bedrock adapter for Instructor using the Converse API.

  Uses the unified Converse API which provides a consistent interface across
  all Bedrock models (Claude, Llama, Titan, Mistral, etc.).

  ## Configuration

  Configure the Bedrock adapter with bearer token authentication:

      config :instructor,
        adapter: Instructor.Adapters.Bedrock,
        bedrock: [
          api_key: "your_bearer_token",  # defaults to AWS_BEARER_TOKEN_BEDROCK env var
          auth_mode: :bearer,            # authentication mode (default: :bearer)
          http_options: [receive_timeout: 60_000]
        ]

  The region is **auto-detected from the bearer token**. You can override it:

      config :instructor,
        adapter: Instructor.Adapters.Bedrock,
        bedrock: [
          region: "us-west-2",           # optional: overrides auto-detection
          api_key: "your_bearer_token"
        ]

  Region priority: explicit config > AWS_REGION env var > auto-detected from token > "us-east-1"

  You can also provide a custom runtime URL or dynamic token:

      config :instructor,
        adapter: Instructor.Adapters.Bedrock,
        bedrock: [
          runtime_url: "https://custom-bedrock-endpoint.example.com",
          api_key: fn -> get_dynamic_token() end
        ]

  ## Usage

      Instructor.chat_completion(
        model: "anthropic.claude-3-5-sonnet-20240620-v1:0",
        response_model: MySchema,
        messages: [%{role: "user", content: "..."}]
      )

  ## Supported Models

  All models that support the Bedrock Converse API:
  - **Anthropic Claude**: `anthropic.claude-*`
  - **Meta Llama**: `meta.llama*`
  - **Amazon Titan**: `amazon.titan-*`
  - **Cohere**: `cohere.*`
  - **Mistral**: `mistral.*`
  """

  @behaviour Instructor.Adapter

  alias Instructor.AWSEventStreamParser

  @supported_modes [:tools, :json, :md_json]
  @default_max_tokens 4096

  @impl true
  def chat_completion(params, user_config \\ nil) do
    config = config(user_config)
    model_id = Keyword.fetch!(params, :model)
    messages = Keyword.fetch!(params, :messages)
    mode = Keyword.get(params, :mode, :tools)
    max_tokens = Keyword.get(params, :max_tokens, @default_max_tokens)
    temperature = Keyword.get(params, :temperature, 1.0)
    tools = Keyword.get(params, :tools, [])
    stream = Keyword.get(params, :stream, false)

    if mode not in @supported_modes do
      raise "Unsupported Bedrock mode #{mode}. Supported modes: #{inspect(@supported_modes)}"
    end

    body = build_converse_body(messages, max_tokens, temperature, tools)

    if stream do
      do_streaming_chat_completion(mode, model_id, body, config)
    else
      do_chat_completion(mode, model_id, body, tools, config)
    end
  end

  defp do_chat_completion(mode, model_id, body, tools, config) do
    case converse(model_id, body, config) do
      {:ok, response} ->
        parse_response(mode, response, tools)

      {:error, _reason} = error ->
        error
    end
  end

  defp do_streaming_chat_completion(mode, model_id, body, config) do
    pid = self()
    ref = make_ref()
    url = build_stream_url(model_id, config)
    options = build_stream_request_options(body, config)

    Stream.resource(
      fn ->
        Task.async(fn ->
          options =
            Keyword.merge(options,
              into: fn {:data, data}, {req, resp} ->
                send(pid, {ref, data})
                {:cont, {req, resp}}
              end
            )

          Req.post(url, options)
          send(pid, {ref, :done})
        end)
      end,
      fn task ->
        receive do
          {^ref, :done} ->
            {:halt, task}

          {^ref, data} ->
            {[data], task}
        after
          30_000 ->
            raise "Timeout waiting for Bedrock streaming response"
        end
      end,
      fn _ -> nil end
    )
    |> AWSEventStreamParser.parse()
    |> Stream.map(&parse_stream_chunk_for_mode(mode, &1))
  end

  @impl true
  def reask_messages(raw_response, params, _config) do
    reask_messages_for_mode(params[:mode], raw_response)
  end

  defp reask_messages_for_mode(:tools, %{
         "choices" => [
           %{
             "message" => %{
               "tool_calls" => [
                 %{
                   "id" => tool_call_id,
                   "function" => %{"name" => _name, "arguments" => args}
                 }
               ]
             }
           }
         ]
       }) do
    assistant_message = %{
      role: "assistant",
      __bedrock_tool_use__: %{
        "toolUseId" => tool_call_id,
        "input" => args
      }
    }

    tool_result_message = %{
      role: "user",
      __bedrock_tool_result__: %{
        "toolUseId" => tool_call_id,
        "content" => args
      }
    }

    [assistant_message, tool_result_message]
  end

  defp reask_messages_for_mode(_mode, _raw_response), do: []

  # Converse API Request Building
  defp build_converse_body(messages, max_tokens, temperature, tools) do
    {system_messages, user_messages} = extract_system_messages(messages)

    %{
      "messages" => format_messages(user_messages),
      "inferenceConfig" => %{
        "maxTokens" => max_tokens,
        "temperature" => temperature
      }
    }
    |> maybe_add_system(system_messages)
    |> maybe_add_tools(tools)
  end

  defp extract_system_messages(messages) do
    Enum.split_with(messages, &is_system_message?/1)
  end

  defp is_system_message?(%{role: role}), do: role in ["system", :system]
  defp is_system_message?(%{"role" => "system"}), do: true
  defp is_system_message?(_), do: false

  defp format_messages(messages) do
    Enum.map(messages, &format_message/1)
  end

  defp format_message(%{__bedrock_tool_use__: tool_use} = msg) do
    input =
      case tool_use["input"] do
        input when is_binary(input) -> Jason.decode!(input)
        input when is_map(input) -> input
      end

    %{
      "role" => to_string(msg.role),
      "content" => [
        %{
          "toolUse" => %{
            "toolUseId" => tool_use["toolUseId"],
            "name" => "Schema",
            "input" => input
          }
        }
      ]
    }
  end

  defp format_message(%{__bedrock_tool_result__: tool_result} = msg) do
    %{
      "role" => to_string(msg.role),
      "content" => [
        %{
          "toolResult" => %{
            "toolUseId" => tool_result["toolUseId"],
            "content" => [%{"text" => tool_result["content"]}]
          }
        }
      ]
    }
  end

  defp format_message(msg) do
    %{
      "role" => get_role(msg),
      "content" => [%{"text" => get_content(msg)}]
    }
  end

  defp get_role(%{role: role}), do: to_string(role)
  defp get_role(%{"role" => role}), do: role

  defp get_content(%{content: content}), do: content
  defp get_content(%{"content" => content}), do: content

  defp maybe_add_system(body, []), do: body

  defp maybe_add_system(body, system_messages) do
    system_messages
    |> Enum.map(&get_content/1)
    |> Enum.map(&%{"text" => &1})
    |> then(&Map.put(body, "system", &1))
  end

  defp maybe_add_tools(body, []), do: body

  defp maybe_add_tools(body, tools) do
    formatted_tools = Enum.map(tools, &format_tool/1)

    first_tool_name =
      case formatted_tools do
        [%{"toolSpec" => %{"name" => name}} | _] -> name
        _ -> nil
      end

    tool_config = %{
      "tools" => formatted_tools
    }

    tool_config =
      if first_tool_name do
        Map.put(tool_config, "toolChoice", %{"tool" => %{"name" => first_tool_name}})
      else
        tool_config
      end

    Map.put(body, "toolConfig", tool_config)
  end

  defp format_tool(tool) do
    function = tool[:function] || tool["function"]

    %{
      "toolSpec" => %{
        "name" => function[:name] || function["name"],
        "description" => function[:description] || function["description"],
        "inputSchema" => %{
          "json" => function[:parameters] || function["parameters"]
        }
      }
    }
  end

  defp parse_response(mode, response, tools) do
    raw_response = build_raw_response(response, tools)

    case parse_content_for_mode(mode, raw_response) do
      {:ok, parsed} -> {:ok, raw_response, parsed}
      {:error, _} = error -> error
    end
  end

  defp build_raw_response(response, tools) do
    output = response["output"] || %{}
    message = output["message"] || %{}
    content = message["content"] || []
    stop_reason = response["stopReason"]

    tool_use = Enum.find(content, &(&1["toolUse"] != nil))

    if tool_use && tools != [] do
      tool_use_block = tool_use["toolUse"]

      %{
        "choices" => [
          %{
            "message" => %{
              "tool_calls" => [
                %{
                  "id" => tool_use_block["toolUseId"],
                  "function" => %{
                    "name" => tool_use_block["name"],
                    "arguments" => Jason.encode!(tool_use_block["input"])
                  }
                }
              ]
            },
            "finish_reason" => normalize_stop_reason(stop_reason)
          }
        ]
      }
    else
      text_content =
        content
        |> Enum.filter(&(&1["text"] != nil))
        |> Enum.map(& &1["text"])
        |> Enum.join("")

      %{
        "choices" => [
          %{
            "message" => %{
              "content" => text_content
            },
            "finish_reason" => normalize_stop_reason(stop_reason)
          }
        ]
      }
    end
  end

  defp normalize_stop_reason("end_turn"), do: "stop"
  defp normalize_stop_reason("tool_use"), do: "tool_calls"
  defp normalize_stop_reason("max_tokens"), do: "length"
  defp normalize_stop_reason(other), do: other

  defp parse_content_for_mode(:tools, %{
         "choices" => [
           %{"message" => %{"tool_calls" => [%{"function" => %{"arguments" => args}}]}}
         ]
       }) do
    Jason.decode(args)
  end

  defp parse_content_for_mode(:json, %{"choices" => [%{"message" => %{"content" => content}}]}) do
    Jason.decode(content)
  end

  defp parse_content_for_mode(:md_json, %{"choices" => [%{"message" => %{"content" => content}}]}) do
    extract_json_from_markdown(content)
  end

  defp parse_content_for_mode(mode, response) do
    {:error, "Unsupported mode #{mode} with response #{inspect(response)}"}
  end

  defp extract_json_from_markdown(content) do
    case Regex.run(~r/```(?:json)?\s*([\s\S]*?)\s*```/, content) do
      [_, json] -> Jason.decode(json)
      nil -> Jason.decode(content)
    end
  end

  # ---------------------------------------------------------
  # Streaming Response Parsing
  # ---------------------------------------------------------

  # Tool use streaming - input comes as string chunks
  defp parse_stream_chunk_for_mode(:tools, %{"delta" => %{"toolUse" => %{"input" => chunk}}}) do
    chunk
  end

  # Text streaming
  defp parse_stream_chunk_for_mode(_mode, %{"delta" => %{"text" => chunk}}) do
    chunk
  end

  # Skip non-content events
  defp parse_stream_chunk_for_mode(_mode, %{"role" => _}), do: ""
  defp parse_stream_chunk_for_mode(_mode, %{"stopReason" => _}), do: ""
  defp parse_stream_chunk_for_mode(_mode, %{"usage" => _}), do: ""
  defp parse_stream_chunk_for_mode(_mode, %{"metrics" => _}), do: ""
  defp parse_stream_chunk_for_mode(_mode, _), do: ""

  # ---------------------------------------------------------
  # HTTP / Converse API
  # ---------------------------------------------------------

  @doc false
  def converse(model_id, body, config) do
    url = build_url(model_id, config)
    options = build_request_options(body, config)

    case Req.post(url, options) do
      {:ok, %Req.Response{status: 200, body: response_body}} ->
        {:ok, response_body}

      {:ok, %Req.Response{status: status, body: error_body}} ->
        {:error, "Unexpected HTTP response code: #{status}\n#{inspect(error_body)}"}

      {:error, reason} ->
        {:error, "Bedrock request failed: #{inspect(reason)}"}
    end
  end

  @doc false
  def build_url(model_id, config) do
    base_url = config[:runtime_url] || "https://bedrock-runtime.#{config[:region]}.amazonaws.com"
    path = "/model/#{URI.encode(model_id)}/converse"
    base_url <> path
  end

  defp build_stream_url(model_id, config) do
    base_url = config[:runtime_url] || "https://bedrock-runtime.#{config[:region]}.amazonaws.com"
    path = "/model/#{URI.encode(model_id)}/converse-stream"
    base_url <> path
  end

  defp build_base_request_options(body, config, accept, extra_opts \\ []) do
    http_options = Keyword.get(config, :http_options, [])

    Keyword.merge(
      http_options,
      [
        headers: %{
          "content-type" => "application/json",
          "accept" => accept
        },
        auth: auth_header(config),
        json: body
      ] ++ extra_opts
    )
  end

  defp build_request_options(body, config) do
    build_base_request_options(body, config, "application/json")
  end

  defp build_stream_request_options(body, config) do
    build_base_request_options(body, config, "application/vnd.amazon.eventstream",
      decode_body: false
    )
  end

  defp auth_header(config) do
    case Keyword.get(config, :auth_mode, :bearer) do
      :bearer -> {:bearer, api_key(config)}
    end
  end

  defp api_key(config) do
    case Keyword.get(config, :api_key) do
      fun when is_function(fun, 0) -> fun.()
      key -> key
    end
  end

  defp config(nil), do: config(Application.get_env(:instructor, :bedrock, []))

  defp config(base_config) do
    api_key = Keyword.get(base_config, :api_key) || System.get_env("AWS_BEARER_TOKEN_BEDROCK")

    # Auto-detect region from token if not explicitly provided
    region =
      Keyword.get(base_config, :region) ||
        System.get_env("AWS_REGION") ||
        extract_region_from_token(api_key)

    default_config = [
      region: region,
      runtime_url: nil,
      api_key: api_key,
      auth_mode: :bearer,
      http_options: [receive_timeout: 60_000]
    ]

    Keyword.merge(default_config, base_config)
  end

  defp extract_region_from_token(nil), do: nil

  defp extract_region_from_token(token) when is_binary(token) do
    # Token format: bedrock-api-key-{base64_encoded_content}
    # The base64 content contains X-Amz-Credential with region info
    with "bedrock-api-key-" <> base64_part <- token,
         {:ok, decoded} <- Base.decode64(base64_part),
         [_, region] <- Regex.run(~r/X-Amz-Credential=[^%]+%2F\d+%2F([^%]+)%2F/, decoded) do
      URI.decode(region)
    else
      _ -> nil
    end
  end

  defp extract_region_from_token(_), do: nil
end
