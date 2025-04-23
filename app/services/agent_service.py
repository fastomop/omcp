import requests
from typing import Dict, Any, Optional
import json

from app.core.config import settings
from app.core.logging_setup import logger
from app.api.models import AgentIntegration


class AgentService:
    """Service for integrating with external AI agents"""

    async def get_agent_insights(self, prompt: str, sql: str,
                                 agent_integration: AgentIntegration) -> Dict[str, Any]:
        """Get insights from an external agent"""
        try:
            # Get agent URL either from request or from config
            agent_url = agent_integration.agent_url
            if not agent_url:
                try:
                    agent_config = settings.get_agent_config(agent_integration.agent_type)
                    agent_url = agent_config["url"]
                    timeout = agent_config.get("timeout", 30)
                except ValueError:
                    return {"error": f"No URL configured for agent type {agent_integration.agent_type}"}
            else:
                timeout = 30  # Default timeout

            # Prepare context for the agent
            context = agent_integration.context or {}
            context.update({
                "original_prompt": prompt,
                "generated_sql": sql,
                "agent_type": agent_integration.agent_type
            })

            # Call the agent
            logger.info(f"Calling external agent: {agent_integration.agent_type}")
            response = requests.post(agent_url, json=context, timeout=timeout)
            response.raise_for_status()

            return response.json()

        except requests.RequestException as e:
            logger.error(f"Error calling agent: {e}")
            return {"error": f"Agent integration error: {str(e)}"}
        except Exception as e:
            logger.error(f"Agent integration error: {e}")
            return {"error": f"Unexpected error: {str(e)}"}

    def get_available_agents(self) -> Dict[str, Dict[str, Any]]:
        """Get list of available agent types"""
        return settings.config.get("agents", {})


# Create a global service instance
agent_service = AgentService()