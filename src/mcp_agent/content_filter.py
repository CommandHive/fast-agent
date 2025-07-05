import re
from typing import Optional, Dict, Any
from dataclasses import dataclass
import logging
import asyncio

logger = logging.getLogger(__name__)

@dataclass
class FilterResult:
    is_allowed: bool
    reason: Optional[str] = None
    category: Optional[str] = None

class StudentContentFilter:
    def __init__(self, agent_instance=None):
        """Initialize the student content filter with blocked patterns and optional agent"""
        self.agent_instance = agent_instance
        
        # Blocked patterns for students
        self.blocked_patterns = {
            'adult_content': [
                # Adult industry names and terms
                r'\b(lana\s*rho?des?|mia\s*khalifa|johnny\s*sins|riley\s*reid|brandi\s*love)\b',
                r'\b(porn|pornstar|xxx|adult\s*film|adult\s*video|nsfw)\b',
                r'\b(onlyfans|chaturbate|pornhub|xvideos|redtube)\b',
                # Adult content terms
                r'\b(nude|naked|sex|erotic|fetish|stripper|escort)\b',
            ],
            'math_homework': [
                # Direct homework requests
                r'\b(solve\s*(this|my|the)?\s*(math|equation|problem|homework))\b',
                r'\b(what\s*is\s*the\s*answer\s*to|calculate\s*for\s*me)\b',
                r'\b(do\s*my\s*(math\s*)?homework|complete\s*my\s*assignment)\b',
                # Specific math problem patterns
                r'\b(find\s*the\s*(derivative|integral|limit|solution))\b',
                r'\b(solve\s*for\s*[xy]|evaluate\s*the\s*expression)\b',
            ],
            'notion_access': [
                r'\b(notion|notion\.so|notion\s*api)\b',
                r'\b(access\s*notion|connect\s*to\s*notion|use\s*notion)\b',
            ],
            'cheating': [
                r'\b(write\s*my\s*essay|do\s*my\s*assignment|complete\s*my\s*test)\b',
                r'\b(answer\s*sheet|exam\s*answers|test\s*solutions)\b',
                r'\b(plagiari[sz]e|copy\s*from|cheat\s*on)\b',
            ]
        }
        
        # Compile patterns for efficiency
        self.compiled_patterns = {}
        for category, patterns in self.blocked_patterns.items():
            self.compiled_patterns[category] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
    
    def check_blocked_content(self, text: str) -> FilterResult:
        """Check if text contains blocked content"""
        text_lower = text.lower()
        
        for category, patterns in self.compiled_patterns.items():
            for pattern in patterns:
                if pattern.search(text_lower):
                    return FilterResult(
                        is_allowed=False,
                        reason=f"Content blocked: {category.replace('_', ' ')}",
                        category=category
                    )
        
        return FilterResult(is_allowed=True)
    
    def get_safe_response(self, category: str) -> str:
        """Get appropriate response for blocked content"""
        responses = {
            'adult_content': "I cannot provide information about adult content or adult entertainment personalities. Let's discuss something else that's appropriate for your learning.",
            'math_homework': "I'd be happy to help you understand math concepts and guide you through problem-solving methods, but I cannot directly solve homework problems for you. Would you like me to explain the concepts involved instead?",
            'notion_access': "Access to Notion is restricted for student accounts. Please use the approved learning management system provided by your institution.",
            'cheating': "I cannot help with activities that might constitute academic dishonesty. I'm here to help you learn and understand concepts. Would you like help understanding the topic instead?",
            'default': "This content is not available for student accounts. Let's focus on educational topics that can help with your learning."
        }
        
        return responses.get(category, responses['default'])
    
    async def check_content_with_agent(self, text: str) -> FilterResult:
        """Use agent to analyze content for more sophisticated filtering"""
        if not self.agent_instance:
            logger.warning("No agent instance available for advanced filtering")
            return FilterResult(is_allowed=True)
        
        try:
            # Create a prompt for the agent to analyze content
            analysis_prompt = f"""
            Analyze the following text for inappropriate content that should be blocked for student accounts.
            
            Categories to check for:
            1. Adult content (pornography, adult entertainment, explicit material)
            2. Math homework cheating (direct problem solving requests)
            3. Academic dishonesty (essay writing, test answers, plagiarism)
            4. Inappropriate requests for restricted services (Notion access for students)
            
            Text to analyze: "{text}"
            
            Respond with ONLY a JSON object in this exact format:
            {{"blocked": true/false, "category": "category_name_or_null", "reason": "brief_reason_or_null"}}
            
            Be strict about blocking content that violates student policies.
            """
            
            # Get response from agent
            response = await self.agent_instance.assistant(analysis_prompt)
            
            # Parse the JSON response
            import json
            try:
                # Extract JSON from response if it contains other text
                response_clean = response.strip()
                if response_clean.startswith('```json'):
                    response_clean = response_clean.split('```json')[1].split('```')[0].strip()
                elif response_clean.startswith('```'):
                    response_clean = response_clean.split('```')[1].split('```')[0].strip()
                
                result = json.loads(response_clean)
                
                if result.get('blocked', False):
                    return FilterResult(
                        is_allowed=False,
                        reason=f"Agent detected: {result.get('reason', 'inappropriate content')}",
                        category=result.get('category', 'agent_detected')
                    )
                else:
                    return FilterResult(is_allowed=True)
                    
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Failed to parse agent response: {e}, response: {response}")
                # Fall back to allowing if we can't parse
                return FilterResult(is_allowed=True)
                
        except Exception as e:
            logger.error(f"Error in agent-based filtering: {e}")
            # Fall back to allowing on error
            return FilterResult(is_allowed=True)
    
    async def check_blocked_content_comprehensive(self, text: str) -> FilterResult:
        """Check content using both pattern matching and agent analysis"""
        # First check with regex patterns (fast)
        pattern_result = self.check_blocked_content(text)
        if not pattern_result.is_allowed:
            return pattern_result
        
        # If patterns don't catch it, use agent for deeper analysis
        agent_result = await self.check_content_with_agent(text)
        return agent_result