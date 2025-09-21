import requests
import json
import hashlib
import os
from datetime import datetime
from pathlib import Path
from collections import defaultdict


class GuildWorldChangeDetector:
    def __init__(self, cache_dir="guild_world_cache"):
        """Initialize the guild world change detector with cache directory."""
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.current_file = self.cache_dir / "current_world_assignments.json"
        self.previous_file = self.cache_dir / "previous_world_assignments.json"
        self.metadata_file = self.cache_dir / "metadata.json"
    
    def fetch_north_american_guilds(self):
        """Fetch listing of North American Guilds from GW2API."""
        url = "https://api.guildwars2.com/v2/wvw/guilds/na"
        try:
            response = requests.get(url, timeout=(3.05, 5))
            response.raise_for_status()
            data = response.json()
            return data
        
        except requests.exceptions.RequestException as error:
            print(f"Error fetching guilds: {error}")
            return None
    
    def calculate_mapping_hash(self, mapping):
        """Calculate a hash of the guild-to-world mapping for quick comparison."""
        if not mapping:
            return None
        # Sort keys for consistent hashing
        sorted_items = sorted(mapping.items())
        data_str = json.dumps(sorted_items)
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()
    
    def save_world_assignments(self, mapping, timestamp=None):
        """Save current world assignments and update previous data."""
        if not mapping:
            return False
        
        timestamp = timestamp or datetime.now()
        
        # Save current mapping
        current_data = {
            'mapping': mapping,
            'timestamp': timestamp.isoformat(),
            'hash': self.calculate_mapping_hash(mapping),
            'guild_count': len(mapping),
            'world_count': len(set(mapping.values()))
        }
        
        with open(self.current_file, 'w') as f:
            json.dump(current_data, f, indent=2)
        
        # Archive previous data if it exists
        if self.previous_file.exists():
            archive_name = f"previous_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
            archive_path = self.cache_dir / archive_name
            self.previous_file.rename(archive_path)
        
        # Update previous to current
        if self.current_file.exists():
            self.current_file.rename(self.previous_file)
        
        # Update metadata
        self._update_metadata(timestamp, current_data)
        
        return True
    
    def _update_metadata(self, timestamp, current_data):
        """Update metadata with run history."""
        metadata = {}
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                metadata = json.load(f)
        
        metadata['last_run'] = timestamp.isoformat()
        metadata['current_hash'] = current_data['hash']
        metadata['guild_count'] = current_data['guild_count']
        metadata['world_count'] = current_data['world_count']
        metadata['run_count'] = metadata.get('run_count', 0) + 1
        
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def has_world_assignments_changed(self):
        """Quick check if world assignments have changed using hash comparison."""
        if not self.previous_file.exists():
            return None  # First run
        
        current_hash = self.get_current_hash()
        previous_hash = self.get_previous_hash()
        
        if current_hash is None or previous_hash is None:
            return None
        
        return current_hash != previous_hash
    
    def get_current_hash(self):
        """Get hash of current mapping."""
        if not self.current_file.exists():
            return None
        with open(self.current_file, 'r') as f:
            data = json.load(f)
            return data.get('hash')
    
    def get_previous_hash(self):
        """Get hash of previous mapping."""
        if not self.previous_file.exists():
            return None
        with open(self.previous_file, 'r') as f:
            data = json.load(f)
            return data.get('hash')
    
    def get_world_assignment_changes(self):
        """Get detailed analysis of world assignment changes."""
        if not self.previous_file.exists() or not self.current_file.exists():
            return None
        
        with open(self.previous_file, 'r') as f:
            prev_data = json.load(f)
            prev_mapping = prev_data['mapping']
        
        with open(self.current_file, 'r') as f:
            curr_data = json.load(f)
            curr_mapping = curr_data['mapping']
        
        changes = {
            'guilds_reassigned': {},
            'guilds_added': [],
            'guilds_removed': [],
            'worlds_gained': set(),
            'worlds_lost': set(),
            'total_guilds_previous': len(prev_mapping),
            'total_guilds_current': len(curr_mapping),
            'total_reassignments': 0
        }
        
        # Track which guilds changed worlds
        for guild_id in set(prev_mapping.keys()) | set(curr_mapping.keys()):
            prev_world = prev_mapping.get(guild_id)
            curr_world = curr_mapping.get(guild_id)
            
            # Guild was added
            if guild_id not in prev_mapping and guild_id in curr_mapping:
                changes['guilds_added'].append({
                    'guild_id': guild_id,
                    'new_world': curr_world
                })
            
            # Guild was removed
            elif guild_id in prev_mapping and guild_id not in curr_mapping:
                changes['guilds_removed'].append({
                    'guild_id': guild_id,
                    'old_world': prev_world
                })
                changes['worlds_lost'].add(prev_world)
            
            # Guild reassigned to different world
            elif prev_world != curr_world:
                changes['guilds_reassigned'][guild_id] = {
                    'old_world': prev_world,
                    'new_world': curr_world
                }
                changes['worlds_lost'].add(prev_world)
                changes['worlds_gained'].add(curr_world)
        
        changes['total_reassignments'] = len(changes['guilds_reassigned'])
        changes['total_changes'] = (len(changes['guilds_added']) + 
                                  len(changes['guilds_removed']) + 
                                  changes['total_reassignments'])
        
        return changes
    
    def get_change_summary(self, changes):
        """Generate a human-readable summary of world assignment changes."""
        if not changes:
            return "No world assignment changes detected."
        
        summary = []
        summary.append(f"Previous: {changes['total_guilds_previous']} guilds across {len(set(prev_mapping.values()))} worlds") # type: ignore
        summary.append(f"Current: {changes['total_guilds_current']} guilds across {len(set(curr_mapping.values()))} worlds") # type: ignore
        summary.append("")
        
        if changes['guilds_reassigned']:
            summary.append(f"🔄 {changes['total_reassignments']} guild(s) reassigned to new worlds:")
            for i, (guild_id, assignment) in enumerate(list(changes['guilds_reassigned'].items())[:5], 1):
                summary.append(f"  {i}. {guild_id}: {assignment['old_world']} → {assignment['new_world']}")
            if len(changes['guilds_reassigned']) > 5:
                summary.append(f"  ... and {len(changes['guilds_reassigned']) - 5} more")
            summary.append("")
        
        if changes['guilds_added']:
            summary.append(f"➕ {len(changes['guilds_added'])} new guild(s) added:")
            for i, guild in enumerate(changes['guilds_added'][:3], 1):
                summary.append(f"  {i}. {guild['guild_id']}: assigned to world {guild['new_world']}")
            if len(changes['guilds_added']) > 3:
                summary.append(f"  ... and {len(changes['guilds_added']) - 3} more")
            summary.append("")
        
        if changes['guilds_removed']:
            summary.append(f"➖ {len(changes['guilds_removed'])} guild(s) removed:")
            for i, guild in enumerate(changes['guilds_removed'][:3], 1):
                summary.append(f"  {i}. {guild['guild_id']}: removed from world {guild['old_world']}")
            if len(changes['guilds_removed']) > 3:
                summary.append(f"  ... and {len(changes['guilds_removed']) - 3} more")
            summary.append("")
        
        if changes['worlds_gained']:
            gained = sorted(changes['worlds_gained'])
            lost = sorted(changes['worlds_lost'])
            summary.append(f"🌍 World changes:")
            if gained:
                summary.append(f"  Gained guilds in worlds: {', '.join(gained)}")
            if lost:
                summary.append(f"  Lost guilds from worlds: {', '.join(lost)}")
        
        return "\n".join(summary)
    
    def get_world_population_changes(self, changes):
        """Analyze how world populations changed due to reassignments."""
        if not changes or not changes['guilds_reassigned']:
            return None
        
        # Get previous and current mappings for world population analysis
        with open(self.previous_file, 'r') as f:
            prev_mapping = json.load(f)['mapping']
        with open(self.current_file, 'r') as f:
            curr_mapping = json.load(f)['mapping']
        
        # Calculate world populations
        prev_world_pop = defaultdict(int)
        curr_world_pop = defaultdict(int)
        
        for guild_id, world_id in prev_mapping.items():
            prev_world_pop[world_id] += 1
        
        for guild_id, world_id in curr_mapping.items():
            curr_world_pop[world_id] += 1
        
        # Calculate net changes
        world_changes = {}
        all_worlds = set(prev_world_pop.keys()) | set(curr_world_pop.keys())
        
        for world_id in all_worlds:
            prev_count = prev_world_pop[world_id]
            curr_count = curr_world_pop[world_id]
            net_change = curr_count - prev_count
            
            if net_change != 0:
                world_changes[world_id] = {
                    'previous_count': prev_count,
                    'current_count': curr_count,
                    'net_change': net_change,
                    'reassignments_in': [],
                    'reassignments_out': []
                }
        
        # Track specific reassignments per world
        for guild_id, assignment in changes['guilds_reassigned'].items():
            old_world = assignment['old_world']
            new_world = assignment['new_world']
            
            if old_world in world_changes:
                world_changes[old_world]['reassignments_out'].append(guild_id)
            if new_world in world_changes:
                world_changes[new_world]['reassignments_in'].append(guild_id)
        
        return {
            'world_changes': dict(sorted(world_changes.items(), 
                                       key=lambda x: abs(x[1]['net_change']), reverse=True)),
            'total_reassigned_guilds': len(changes['guilds_reassigned'])
        }
    
    def run_world_monitor(self, reassign_threshold=1, trigger_downstream=True):
        """
        Main monitoring function for world assignment changes.
        
        Args:
            reassign_threshold: Minimum number of reassignments to trigger downstream (0 = any change)
            trigger_downstream: Whether to actually trigger downstream processes
        
        Returns:
            dict: Results including change detection and summary
        """
        print("Fetching current guild world assignments...")
        current_data = self.fetch_north_american_guilds()
        
        if not current_data:
            print("Failed to fetch guild data. Aborting.")
            return {'success': False, 'error': 'Failed to fetch data'}
        
        print(f"Fetched {len(current_data)} guild-to-world assignments.")
        
        # Save the data
        timestamp = datetime.now()
        save_success = self.save_world_assignments(current_data, timestamp)
        
        if not save_success:
            print("Failed to save data. Aborting.")
            return {'success': False, 'error': 'Failed to save data'}
        
        # Check for changes
        change_detected = self.has_world_assignments_changed()
        
        if change_detected is None:
            print("First run - establishing baseline world assignments.")
            result = {
                'success': True,
                'first_run': True,
                'timestamp': timestamp,
                'total_guilds': len(current_data),
                'unique_worlds': len(set(current_data.values())),
                'should_trigger': False,
                'summary': "Baseline established successfully."
            }
        else:
            if change_detected:
                changes = self.get_world_assignment_changes()
                reassignments = changes['total_reassignments']
                
                should_trigger = reassignments >= reassign_threshold
                summary = self.get_change_summary(changes)
                
                print(f"🚨 WORLD ASSIGNMENT CHANGES DETECTED!")
                print(f"{reassignments} guilds reassigned to new worlds")
                print("\n" + "="*50)
                print(summary)
                print("="*50)
                
                # Get world population analysis
                population_changes = self.get_world_population_changes(changes)
                if population_changes:
                    print(f"\n📊 World population impact:")
                    for world_id, change_data in list(population_changes['world_changes'].items())[:5]:
                        sign = "+" if change_data['net_change'] > 0 else ""
                        print(f"  World {world_id}: {change_data['previous_count']} → {change_data['current_count']} "
                              f"({sign}{change_data['net_change']})")
                
                result = {
                    'success': True,
                    'changed': True,
                    'timestamp': timestamp,
                    'total_guilds': len(current_data),
                    'unique_worlds': len(set(current_data.values())),
                    'total_reassignments': reassignments,
                    'total_changes': changes['total_changes'],
                    'changes': changes,
                    'population_changes': population_changes,
                    'should_trigger': should_trigger,
                    'summary': summary
                }
                
                if should_trigger and trigger_downstream:
                    print(f"\n--- TRIGGERING DOWNSTREAM PROCESSES FOR {reassignments} REASSIGNMENTS ---")
                    self.trigger_downstream_processes(changes, population_changes)
                    
            else:
                print("✅ No world assignment changes detected.")
                result = {
                    'success': True,
                    'changed': False,
                    'timestamp': timestamp,
                    'total_guilds': len(current_data),
                    'unique_worlds': len(set(current_data.values())),
                    'should_trigger': False,
                    'summary': "No world assignment changes detected."
                }
        
        return result
    
    def trigger_downstream_processes(self, changes, population_changes):
        """Process world assignment changes in your downstream systems."""
        print("Running downstream world assignment processing...")
        
        # Process reassignments
        if changes['guilds_reassigned']:
            print(f"Processing {len(changes['guilds_reassigned'])} guild reassignments...")
            for guild_id, assignment in changes['guilds_reassigned'].items():
                print(f"  Updating {guild_id}: {assignment['old_world']} → {assignment['new_world']}")
                # Trigger wvw-teams.py Refactor to update spreadsheet world id's
                # update_guild_world(guild_id, assignment['new_world'])
        
        # Process new guilds
        if changes['guilds_added']:
            print(f"Processing {len(changes['guilds_added'])} new guilds...")
            for guild in changes['guilds_added']:
                print(f"  Adding {guild['guild_id']} to world {guild['new_world']}")
                # Decide what to do with new guilds
                # add_new_guild_world_assignment(guild['guild_id'], guild['new_world'])
        
        # Process removed guilds
        if changes['guilds_removed']:
            print(f"Processing {len(changes['guilds_removed'])} removed guilds...")
            for guild in changes['guilds_removed']:
                print(f"  Removing {guild['guild_id']} from world {guild['old_world']}")
                # Decide what to do with removed guilds
                # remove_guild_world_assignment(guild['guild_id'])
        
        # Process world population updates
        if population_changes and population_changes['world_changes']:
            print(f"Updating world populations for {len(population_changes['world_changes'])} affected worlds...")
            for world_id, change_data in population_changes['world_changes'].items():
                print(f"  World {world_id}: population changed by {change_data['net_change']}")
                # Decide what to do with world population updates
                # update_world_population(world_id, change_data['current_count'])
        
        print("✅ Downstream processing completed.")


