package com.datasphere.runtime.meta;

import com.datasphere.security.*;
import java.util.*;

public class UserAndRoleUtility
{
    public boolean isDisallowedPermission(final ObjectPermission perm) {
        return perm.getType().equals(ObjectPermission.PermissionType.disallow);
    }
    
    public List<ObjectPermission> getDisallowedPermissionFromList(final List<ObjectPermission> perms) {
        final List<ObjectPermission> list = new ArrayList<ObjectPermission>();
        if (perms == null || perms.isEmpty()) {
            return list;
        }
        for (final ObjectPermission perm : perms) {
            if (this.isDisallowedPermission(perm)) {
                list.add(new ObjectPermission(perm));
            }
        }
        return list;
    }
    
    public boolean doesListHasImpliedPerm(final List<ObjectPermission> list, final ObjectPermission inputPerm) {
        if (list == null || list.isEmpty()) {
            return false;
        }
        if (inputPerm == null) {
            return false;
        }
        for (final ObjectPermission perm : list) {
            if (perm.implies(inputPerm)) {
                return true;
            }
        }
        return false;
    }
    
    public boolean doesListHasExactPerm(final List<ObjectPermission> list, final ObjectPermission inputPerm) {
        if (list == null || list.isEmpty()) {
            return false;
        }
        if (inputPerm == null) {
            return false;
        }
        for (final ObjectPermission perm : list) {
            if (perm.equals(inputPerm)) {
                return true;
            }
        }
        return false;
    }
    
    public List<ObjectPermission> getDependentPerms(final ObjectPermission permToBeRevoked, final List<ObjectPermission> permList) {
        final List<ObjectPermission> list1 = this.getDisallowedPermissionFromList(permList);
        final List<ObjectPermission> list2 = new ArrayList<ObjectPermission>();
        for (final ObjectPermission disAllowedPerm : list1) {
            final ObjectPermission reverseDisAllowedPerm = new ObjectPermission(disAllowedPerm, ObjectPermission.PermissionType.allow);
            if (permToBeRevoked.implies(reverseDisAllowedPerm)) {
                list2.add(new ObjectPermission(disAllowedPerm));
            }
        }
        final List<ObjectPermission> list3 = new ArrayList<ObjectPermission>();
        for (final ObjectPermission disAllowedPerm2 : list2) {
            final ObjectPermission revDisAllowedPerm = new ObjectPermission(disAllowedPerm2, ObjectPermission.PermissionType.allow);
            for (final ObjectPermission perm : permList) {
                if (perm.implies(revDisAllowedPerm)) {
                    list3.add(new ObjectPermission(disAllowedPerm2));
                }
            }
        }
        final Set<ObjectPermission> set2 = new HashSet<ObjectPermission>(list2);
        final Set<ObjectPermission> set3 = new HashSet<ObjectPermission>(list3);
        set2.removeAll(set3);
        final List<ObjectPermission> result = new ArrayList<ObjectPermission>();
        result.addAll(set2);
        return result;
    }
}
