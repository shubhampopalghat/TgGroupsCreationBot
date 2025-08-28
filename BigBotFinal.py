# Save this file as BigBotFinal.py
import asyncio
import random
import json
import time
import os
from telethon import TelegramClient
from telethon.tl.functions.channels import CreateChannelRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.errors.rpcerrorlist import FloodWaitError, ChatAdminRequiredError

API_ID = 22566208

API_HASH = "fa18dcf886c0d78f20e849f54be62940"

def get_account_stats_file(phone_number):
    """Get the stats file path for an account"""
    return f"{phone_number.replace('+', '')}_stats.json"

def load_account_stats(phone_number):
    """Load existing account statistics"""
    stats_file = get_account_stats_file(phone_number)
    if os.path.exists(stats_file):
        try:
            with open(stats_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {
        "total_groups_created": 0,
        "groups_created_today": 0,
        "all_group_links": [],
        "last_updated": "",
        "account_info": {}
    }

def save_account_stats(phone_number, stats):
    """Save account statistics"""
    stats_file = get_account_stats_file(phone_number)
    stats["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Failed to save stats for {phone_number}: {e}")

def get_links_file_path(phone_number):
    """Get the links file path for an account"""
    return f"{phone_number.replace('+', '')}_links.txt"

def save_group_link(phone_number, group_title, invite_link):
    """Save a group link to the account's links file"""
    links_file = get_links_file_path(phone_number)
    try:
        with open(links_file, 'a', encoding='utf-8') as f:
            f.write(f"{invite_link}\n")
        print(f"Link saved to {links_file}")
    except Exception as e:
        print(f"Failed to save link to {links_file}: {e}")

async def safe_sleep(seconds: int, reason: str = ""):
    """Safe sleep with logging"""
    if seconds > 0:
        print(f"Sleeping {seconds}s - {reason}")
        await asyncio.sleep(seconds)

async def account_worker(account_info, groups_to_create, messages_to_send, delay, progress_queue):
    session_path = account_info['session_path']
    phone_number = account_info.get('phone', 'session').replace('+', '')
    account_details = "Could not log in."
    total_created_this_run = 0
    
    # Load existing account statistics
    account_stats = load_account_stats(phone_number)
    
    # Get the links file path
    links_file = get_links_file_path(phone_number)
    
    # Create links file if it doesn't exist
    if not os.path.exists(links_file):
        open(links_file, 'w', encoding='utf-8').close()

    try:
        # Connect to client
        client = TelegramClient(session_path, API_ID, API_HASH)
        await client.connect()
        
        # Check if authorized
        if not await client.is_user_authorized():
            print(f"Account {phone_number} not authorized")
            await client.disconnect()
            return {
                "created_count": 0,
                "account_details": "Session expired or invalid",
                "output_file": None,
                "total_groups_created": account_stats["total_groups_created"]
            }
        
        # Get account details (but don't log them to avoid detection)
        try:
            me = await client.get_me()
            account_details = (
                f"👤 **Name:** {me.first_name} {me.last_name or ''}\n"
                f"🔖 **Username:** @{me.username or 'N/A'}\n"
                f"🆔 **ID:** `{me.id}`"
            )
            
            # Update account info in stats
            account_stats["account_info"] = {
                "name": f"{me.first_name} {me.last_name or ''}".strip(),
                "username": me.username or 'N/A',
                "id": me.id
            }
            
            print(f"Account loaded: {me.first_name} (@{me.username})")
        except Exception as e:
            print(f"Could not get account details: {e}")
            account_details = "Account details unavailable"
        
        # Reduced initial delay after login to avoid immediate automation detection
        print("Waiting 20 seconds after login to avoid account freezing...")
        await safe_sleep(20, "Reduced safety delay after login")
        
        for i in range(groups_to_create):
            try:
                # Random group title to avoid pattern detection
                adjectives = ['Golden', 'Silent', 'Hidden', 'Secret', 'Private', 'Elite', 'Premium', 'Exclusive']
                nouns = ['Oasis', 'Sanctuary', 'Valley', 'Garden', 'Haven', 'Retreat', 'Club', 'Society']
                group_title = f"{random.choice(adjectives)} {random.choice(nouns)} {random.randint(100, 999)}"
                
                print(f"Creating group {i+1}/{groups_to_create}: {group_title}")
                
                # Create group with random delay
                result = await client(CreateChannelRequest(
                    title=group_title, 
                    about="Welcome to our community!", 
                    megagroup=True
                ))
                new_group = result.chats[0]
                
                # Reduced delay after group creation
                await safe_sleep(random.randint(5, 10), f"Delay after creating group {group_title}")
                
                # Get invite link
                try:
                    invite_result = await client(ExportChatInviteRequest(new_group.id))
                    invite_link = invite_result.link
                    
                    # Save link to file
                    save_group_link(phone_number, group_title, invite_link)
                    
                    # Update account statistics
                    account_stats["all_group_links"].append({
                        "title": group_title,
                        "link": invite_link,
                        "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
                    account_stats["total_groups_created"] += 1
                    account_stats["groups_created_today"] += 1
                    
                    print(f"Invite link generated for {group_title}")
                except ChatAdminRequiredError:
                    print(f"Could not export invite for {group_title} - admin rights issue")
                    continue
                
                # Send messages with proper delays
                for j, msg in enumerate(messages_to_send):
                    try:
                        await client.send_message(new_group.id, msg)
                        print(f"Sent message {j+1}/{len(messages_to_send)} to {group_title}")
                        
                        # Random delay between messages (2-5 seconds)
                        if j < len(messages_to_send) - 1:
                            msg_delay = random.randint(2, 5)
                            await safe_sleep(msg_delay, f"Delay between messages in {group_title}")
                    except Exception as e:
                        print(f"Failed to send message {j+1} to {group_title}: {e}")
                        continue
                
                # Note: Commands message removed - should not be sent to groups automatically
                # Users can access bot commands directly in the bot chat
                
                total_created_this_run += 1
                progress_queue.put(1)
                
                # Reduced delay between groups (5-10 seconds) for faster creation
                if i < groups_to_create - 1:
                    group_delay = random.randint(5, 10)
                    await safe_sleep(group_delay, f"Delay before next group")
                
                # Reduced safety delay every 5 groups (10-20 seconds)
                if (i + 1) % 5 == 0:
                    safety_delay = random.randint(10, 20)
                    await safe_sleep(safety_delay, f"Safety delay after {i+1} groups")
                
            except FloodWaitError as fwe:
                print(f"FloodWait for {group_title}: sleeping {fwe.seconds}s")
                await safe_sleep(fwe.seconds + 10, "FloodWait recovery")
                continue
            except Exception as e:
                print(f"Error creating group {group_title}: {e}")
                continue
        
        # Save updated account statistics
        save_account_stats(phone_number, account_stats)
        
        # Final delay before disconnecting
        await safe_sleep(10, "Final delay before disconnecting")
        
    except Exception as e:
        print(f"FATAL ERROR for {phone_number}: {e}")
    finally:
        try:
            await client.disconnect()
        except:
            pass
        
        return {
            "created_count": total_created_this_run,
            "account_details": account_details,
            "output_file": links_file if total_created_this_run > 0 else None,
            "total_groups_created": account_stats["total_groups_created"]
        }

async def run_group_creation_process(account_config, total_groups, msgs_per_group, delay, messages, progress_queue):
    results = await asyncio.gather(account_worker(account_config, total_groups, messages[:msgs_per_group], delay, progress_queue))
    progress_queue.put(f"DONE:{json.dumps(results)}")

def get_account_summary(phone_number):
    """Get a summary of account statistics"""
    stats = load_account_stats(phone_number)
    links_file = get_links_file_path(phone_number)
    
    # Count total lines in links file (each line is a group link)
    total_links = 0
    if os.path.exists(links_file):
        try:
            with open(links_file, 'r', encoding='utf-8') as f:
                total_links = len([line.strip() for line in f if line.strip()])
        except:
            pass
    
    return {
        "phone_number": phone_number,
        "total_groups_created": stats["total_groups_created"],
        "groups_created_today": stats["groups_created_today"],
        "total_links_in_file": total_links,
        "last_updated": stats["last_updated"],
        "account_info": stats["account_info"]
    }